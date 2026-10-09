package authz_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	errorsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/errors/v1"
	adminv1 "github.com/gstamatakis95/engram/gen/go/memory/admin/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/quota"
)

const (
	acme   id.TenantID = "acme"
	globex id.TenantID = "globex"
	tdel   id.TenantID = "tdel" // a tenant whose DeleteTenant ack has committed (state deleting)
)

// tenantFixture is the TenantLookup of the tests: acme and globex are active, tdel is deleting, anything else unknown.
func tenantFixture() authz.TenantLookup {
	return func(_ context.Context, t id.TenantID) (string, error) {
		switch t {
		case acme, globex:
			return "active", nil
		case tdel:
			return "deleting", nil
		}
		return "", errs.NotFoundTenant(t)
	}
}

var (
	a1, a2, aDeleting, aGone = nsID(0x0a), nsID(2), nsID(3), nsID(4)
	g1, aTenantDel, ghost    = nsID(5), nsID(6), nsID(0x7f)
)

func entry(ns id.NamespaceID, t id.TenantID, st catalog.NamespaceState, group string) *catalog.Entry {
	return &catalog.Entry{Namespace: ns, Tenant: t, Name: "n", Shard: 3, Epoch: 7, State: st, Group: group,
		TenantEntry: &catalog.TenantEntry{Tenant: t, State: "active"}}
}

func fixtureResolver() *catalog.StaticResolver {
	td := entry(aTenantDel, acme, catalog.StateActive, "")
	td.TenantEntry.State = "deleting"
	return catalog.NewStaticResolver(
		entry(a1, acme, catalog.StateActive, "eng"), entry(a2, acme, catalog.StateActive, "sales"),
		entry(aDeleting, acme, catalog.StateDeleting, ""), entry(aGone, acme, catalog.StateDeleted, ""),
		entry(g1, globex, catalog.StateActive, ""), td)
}

func claims(t id.TenantID, scopes ...authz.Scope) *authz.Claims {
	return &authz.Claims{Subject: "u-" + string(t), Tenant: t, Scopes: scopes,
		ExpiresAt: time.Now().Add(time.Hour)}
}

func fixtureVerifier() mapVerifier {
	rd, wr := authz.ScopeMemoryRead, authz.ScopeMemoryWrite
	star := func(c *authz.Claims) *authz.Claims { c.AllNamespaces = true; return c }
	one := func(c *authz.Claims, ns id.NamespaceID) *authz.Claims { c.Namespaces = []id.NamespaceID{ns}; return c }
	grp := func(c *authz.Claims, g string) *authz.Claims { c.Groups = []string{g}; return c }
	cellTok := claims("", authz.ScopeWorker)
	cellTok.Cell = "cell-a"
	return mapVerifier{
		claims: map[string]*authz.Claims{
			"read-a1":           one(claims(acme, rd), a1),
			"write-a1":          one(claims(acme, rd, wr), a1),
			"read-star":         star(claims(acme, rd)),
			"read-sales":        grp(claims(acme, rd), "sales"),
			"admin-star":        star(claims(acme, authz.ScopeMemoryAdmin, rd, wr)),
			"read-star-globex":  star(claims(globex, rd)),
			"tenant-admin-acme": claims(acme, authz.ScopeTenantAdmin),
			"operator":          claims("", authz.ScopeOperator),
			"worker-a":          cellTok,
			"admin-ns-only":     one(claims(acme, authz.ScopeMemoryAdmin), a1),
			"tdel-admin": star(claims(tdel, authz.ScopeMemoryAdmin, authz.ScopeMemoryRead,
				authz.ScopeTenantAdmin)),
			"operator-and-tenant-admin": claims("", authz.ScopeOperator, authz.ScopeTenantAdmin),
		},
		failed: map[string]error{"expired": errs.Unauthenticated("token expired")},
	}
}

// authzCase is one row of the table: (claims, method, catalog entry) -> outcome.
type authzCase struct {
	name       string
	token      string
	method     string
	req        proto.Message
	want       error // the error the call must fail with; nil means the interceptor lets it through
	scope      *id.Scope
	handlerErr error // what the handler returns when reached (the N128 cases)
	limiter    quota.Limiter
	before     func(*catalog.StaticResolver)
	cells      authz.CellLookup
	golden     string // testdata/errors/<golden>.json: the wire form of the refusal is pinned there too
}

func recall(t id.TenantID, ns id.NamespaceID) proto.Message {
	return &memoryv1.RecallRequest{Namespace: ref(t, ns), Query: "q"}
}
func getMemory(t id.TenantID, ns id.NamespaceID) proto.Message {
	return &memoryv1.GetMemoryRequest{Namespace: ref(t, ns), MemoryId: "m"}
}
func retain(t id.TenantID, ns id.NamespaceID) proto.Message {
	return &memoryv1.RetainRequest{Namespace: ref(t, ns)}
}

const (
	mRecall       = "/memory.v1.MemoryService/Recall"
	mGetMemory    = "/memory.v1.MemoryService/GetMemory"
	mRetain       = "/memory.v1.MemoryService/Retain"
	mGetOp        = "/memory.v1.OperationService/GetOperation"
	mWaitOp       = "/memory.v1.OperationService/WaitOperation"
	mCreateNS     = "/memory.v1.NamespaceService/CreateNamespace"
	mListNS       = "/memory.v1.NamespaceService/ListNamespaces"
	mDeleteNS     = "/memory.v1.NamespaceService/DeleteNamespace"
	mGetTenant    = "/memory.admin.v1.TenantService/GetTenant"
	mUpdateTenant = "/memory.admin.v1.TenantService/UpdateTenant"
	mGetTenantOp  = "/memory.admin.v1.TenantService/GetTenantOperation"
	mDelTenant    = "/memory.admin.v1.TenantService/DeleteTenant"
	mListTenants  = "/memory.admin.v1.TenantService/ListTenants"
	mLimits       = "/memory.admin.v1.TenantService/UpdateTenantLimits"
	mRelease      = "/memory.admin.v1.ShardService/ReleaseNamespace"
	mStartMove    = "/memory.admin.v1.MoveService/StartMove"
	mCleanup      = "/memory.admin.v1.MoveService/CleanupMove"
)

func authzCases() []authzCase {
	allowed := func(ns id.NamespaceID) *id.Scope { return &id.Scope{Tenant: acme, Namespace: ns, Shard: 3, Epoch: 7} }
	notFound := errs.NotFoundNamespace(acme, ghost)
	cellOf := func(c id.CellID) authz.CellLookup {
		return func(context.Context, string, proto.Message) (id.CellID, error) { return c, nil }
	}
	release := &adminv1.ReleaseNamespaceRequest{Namespace: ref(acme, a1), ShardId: 3, Epoch: 7}
	return []authzCase{
		// Authentication.
		{name: "no token", method: mGetMemory, req: getMemory(acme, a1),
			want:   errs.Unauthenticated("missing bearer token"),
			golden: "unauthenticated_missing"},
		{name: "garbage token", token: "garbage", method: mGetMemory, req: getMemory(acme, a1),
			want: errs.Unauthenticated("invalid token")},
		{name: "expired token", token: "expired", method: mGetMemory, req: getMemory(acme, a1),
			want: errs.Unauthenticated("token expired"), golden: "unauthenticated_expired"},

		// Allowed, unary and streaming; the scope is the entry's (tenant, namespace, shard, epoch).
		{name: "read token reads its namespace", token: "read-a1", method: mGetMemory, req: getMemory(acme, a1),
			scope: allowed(a1)},
		{name: "recall streams on a read token", token: "read-a1", method: mRecall, req: recall(acme, a1),
			scope: allowed(a1)},
		{name: "star admits every namespace of the tenant", token: "read-star", method: mGetMemory,
			req: getMemory(acme, a2), scope: allowed(a2)},
		{name: "group claim admits the namespace of that group", token: "read-sales", method: mGetMemory,
			req: getMemory(acme, a2), scope: allowed(a2)},

		// The tenant oracle: another tenant's namespace, a missing one and a deleted one answer identically.
		{name: "another tenant's namespace is NOT_FOUND", token: "read-star", method: mGetMemory,
			req: getMemory(acme, g1), want: errs.NotFoundNamespace(acme, g1)},
		{name: "another tenant's namespace with its own tenant id in the request", token: "read-star",
			method: mGetMemory, req: getMemory(globex, g1), want: errs.NotFoundNamespace(globex, g1)},
		{name: "unknown namespace is NOT_FOUND", token: "read-star", method: mGetMemory, req: getMemory(acme, ghost),
			want: notFound},
		{name: "deleted namespace is NOT_FOUND", token: "read-star", method: mGetMemory, req: getMemory(acme, aGone),
			want: errs.NotFoundNamespace(acme, aGone)},
		{name: "star of another tenant admits nothing here", token: "read-star-globex", method: mGetMemory,
			req: getMemory(acme, a1), want: errs.NotFoundNamespace(acme, a1), golden: "not_found_namespace"},

		// The allowlist and scopes: PERMISSION_DENIED.
		{name: "same tenant outside the allowlist", token: "read-a1", method: mGetMemory, req: getMemory(acme, a2),
			want: errs.NamespaceNotAllowed(a2)},
		{name: "group of another namespace does not admit", token: "read-sales", method: mGetMemory,
			req: getMemory(acme, a1), want: errs.NamespaceNotAllowed(a1), golden: "permission_denied_allowlist"},
		{name: "missing scope", token: "read-a1", method: mRetain, req: retain(acme, a1),
			want: errs.PermissionDenied("memory.write"), golden: "permission_denied_scope"},
		{name: "scope is checked after the allowlist", token: "read-a1", method: mRetain, req: retain(acme, a2),
			want: errs.NamespaceNotAllowed(a2)},
		{name: "memory.admin is a different scope from memory.write", token: "admin-ns-only", method: mRetain,
			req: retain(acme, a1), want: errs.PermissionDenied("memory.write")},

		// deleting: FAILED_PRECONDITION except the operation reads (N70).
		{name: "deleting namespace rejects reads", token: "read-star", method: mGetMemory,
			req: getMemory(acme, aDeleting),
			want: errs.PreconditionFailed(errs.PreconditionNamespaceDeleting, aDeleting.String(),
				"the namespace is being deleted")},
		{name: "GetOperation is allowed while deleting", token: "read-star", method: mGetOp,
			req:   &memoryv1.GetOperationRequest{Namespace: ref(acme, aDeleting), OperationId: "o"},
			scope: allowed(aDeleting)},
		{name: "WaitOperation is allowed while deleting", token: "read-star", method: mWaitOp,
			req:   &memoryv1.WaitOperationRequest{Namespace: ref(acme, aDeleting), OperationId: "o"},
			scope: allowed(aDeleting)},
		{name: "tenant deleting rejects reads", token: "read-star", method: mGetMemory,
			req:  getMemory(acme, aTenantDel),
			want: errs.PreconditionFailed(errs.PreconditionTenantDeleting, "acme", "the tenant is being deleted")},

		// Quota.
		{name: "recall bucket exhausted", token: "read-star", method: mRecall, req: recall(acme, a1),
			limiter: denyLimiter{deny: map[quota.Bucket]time.Duration{quota.BucketRecall: 1500 * time.Millisecond}},
			want: errs.QuotaExceeded("recalls_per_min", memoryv1.QuotaScope_QUOTA_SCOPE_NAMESPACE, 0, 0,
				1500*time.Millisecond)},
		{name: "retain bucket does not meter recall", token: "read-star", method: mRecall, req: recall(acme, a1),
			limiter: denyLimiter{deny: map[quota.Bucket]time.Duration{quota.BucketRetain: time.Second}},
			scope:   allowed(a1)},

		// Catalog availability and stale cache entries.
		{name: "catalog miss while down is UNAVAILABLE", token: "read-star", method: mGetMemory,
			req:    getMemory(acme, a1),
			before: func(r *catalog.StaticResolver) { r.Fail(catalog.ErrUnavailable()) },
			want:   errs.Unavailable("catalog unavailable", 2*time.Second), golden: "unavailable_catalog"},
		{name: "a stale entry is what the scope carries (the shard's fence corrects it)", token: "read-star",
			method: mGetMemory, req: getMemory(acme, a1),
			before: func(r *catalog.StaticResolver) {
				old := entry(a1, acme, catalog.StateActive, "eng")
				old.Epoch, old.Shard = 6, 2
				r.Stale(old)
			},
			scope: &id.Scope{Tenant: acme, Namespace: a1, Shard: 2, Epoch: 6}},

		// Tenant-level methods.
		{name: "CreateNamespace within the token's tenant", token: "admin-star", method: mCreateNS,
			req: &memoryv1.CreateNamespaceRequest{TenantId: "acme"}, scope: &id.Scope{Tenant: acme}},
		{name: "CreateNamespace in another tenant", token: "admin-star", method: mCreateNS,
			req: &memoryv1.CreateNamespaceRequest{TenantId: "globex"}, want: errs.NotFoundTenant(globex)},
		{name: "CreateNamespace needs memory.admin", token: "read-star", method: mCreateNS,
			req: &memoryv1.CreateNamespaceRequest{TenantId: "acme"}, want: errs.PermissionDenied("memory.admin")},
		{name: "ListNamespaces is memory.read", token: "read-a1", method: mListNS,
			req: &memoryv1.ListNamespacesRequest{TenantId: "acme"}, scope: &id.Scope{Tenant: acme}},
		{name: "DeleteNamespace needs memory.admin", token: "write-a1", method: mDeleteNS,
			req:  &memoryv1.DeleteNamespaceRequest{Namespace: ref(acme, a1)},
			want: errs.PermissionDenied("memory.admin")},

		// Tenant lifecycle and the operator.
		{name: "tenant.admin gets its own tenant", token: "tenant-admin-acme", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "acme"}, scope: &id.Scope{Tenant: acme}},
		{name: "tenant.admin cannot get another tenant", token: "tenant-admin-acme", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "globex"}, want: errs.NotFoundTenant(globex)},
		{name: "operator gets any tenant on the admin route", token: "operator", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "globex"}, scope: &id.Scope{Tenant: globex}},
		{name: "operator deletes any tenant", token: "operator", method: mDelTenant,
			req:   &adminv1.DeleteTenantRequest{TenantId: "globex", ConfirmTenantId: "globex"},
			scope: &id.Scope{Tenant: globex}},
		{name: "ListTenants is operator only", token: "tenant-admin-acme", method: mListTenants,
			req: &adminv1.ListTenantsRequest{}, want: errs.PermissionDenied("engram.operator")},
		{name: "a tenant cannot raise its own limits", token: "tenant-admin-acme", method: mLimits,
			req:  &adminv1.UpdateTenantLimitsRequest{Patch: &adminv1.Tenant{TenantId: "acme"}},
			want: errs.PermissionDenied("engram.operator")},
		{name: "operator updates limits", token: "operator", method: mLimits,
			req: &adminv1.UpdateTenantLimitsRequest{Patch: &adminv1.Tenant{TenantId: "acme"}}, scope: &id.Scope{}},
		{name: "operator starts a move", token: "operator", method: mStartMove,
			req: &adminv1.StartMoveRequest{Namespace: ref(acme, a1)}, scope: &id.Scope{}},
		{name: "tenant token cannot start a move", token: "read-star", method: mStartMove,
			req: &adminv1.StartMoveRequest{Namespace: ref(acme, a1)}, want: errs.PermissionDenied("engram.operator")},

		// engram.worker: exactly ReleaseNamespace and CleanupMove, bound to the shard's cell (N167).
		{name: "worker releases a namespace of its cell", token: "worker-a", method: mRelease, req: release,
			cells: cellOf("cell-a"), scope: &id.Scope{}},
		{name: "worker of another cell", token: "worker-a", method: mRelease, req: release, cells: cellOf("cell-b"),
			want: errs.WrongCell("cell-a")},
		{name: "worker cleans up a move of its cell", token: "worker-a", method: mCleanup,
			req: &adminv1.CleanupMoveRequest{MoveId: "m"}, cells: cellOf("cell-a"), scope: &id.Scope{}},
		{name: "worker without a cell lookup fails closed", token: "worker-a", method: mRelease, req: release,
			want: errs.Wrap(errs.KindPermissionDenied, "the cell binding of the worker token cannot be verified", nil)},
		{name: "worker token is refused elsewhere", token: "worker-a", method: mStartMove,
			req: &adminv1.StartMoveRequest{Namespace: ref(acme, a1)}, cells: cellOf("cell-a"),
			want: errs.PermissionDenied("engram.operator")},
		{name: "worker token cannot read memory", token: "worker-a", method: mGetMemory, req: getMemory(acme, a1),
			want: errs.NotFoundNamespace(acme, a1)},
		{name: "operator is not cell-bound", token: "operator", method: mRelease, req: release, scope: &id.Scope{}},

		// Tenant-level methods on a deleting tenant (N5, N122): refused, except GetTenantOperation (N70).
		{name: "CreateNamespace in a deleting tenant", token: "tdel-admin", method: mCreateNS,
			req: &memoryv1.CreateNamespaceRequest{TenantId: "tdel"}, want: tenantDeleting()},
		{name: "ListNamespaces of a deleting tenant", token: "tdel-admin", method: mListNS,
			req: &memoryv1.ListNamespacesRequest{TenantId: "tdel"}, want: tenantDeleting()},
		{name: "GetTenant of a deleting tenant", token: "tdel-admin", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "tdel"}, want: tenantDeleting()},
		{name: "UpdateTenant of a deleting tenant", token: "tdel-admin", method: mUpdateTenant,
			req: &adminv1.UpdateTenantRequest{Patch: &adminv1.Tenant{TenantId: "tdel"}}, want: tenantDeleting()},
		{name: "DeleteTenant of a deleting tenant", token: "tdel-admin", method: mDelTenant,
			req: &adminv1.DeleteTenantRequest{TenantId: "tdel", ConfirmTenantId: "tdel"}, want: tenantDeleting()},
		{name: "GetTenantOperation is allowed on a deleting tenant", token: "tdel-admin", method: mGetTenantOp,
			req: &adminv1.GetTenantOperationRequest{TenantId: "tdel"}, scope: &id.Scope{Tenant: tdel}},
		{name: "the operator is refused too on a deleting tenant", token: "operator", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "tdel"}, want: tenantDeleting()},
		{name: "the operator awaits a deleting tenant's operation", token: "operator", method: mGetTenantOp,
			req: &adminv1.GetTenantOperationRequest{TenantId: "tdel"}, scope: &id.Scope{Tenant: tdel}},
		{name: "an unknown tenant is NOT_FOUND for the operator", token: "operator", method: mGetTenant,
			req: &adminv1.GetTenantRequest{TenantId: "nosuch"}, want: errs.NotFoundTenant("nosuch")},

		// Scope monotonicity (review F3): the operator keeps its cross-tenant access with any other scope added.
		{name: "operator with tenant.admin still reaches another tenant", token: "operator-and-tenant-admin",
			method: mGetTenant, req: &adminv1.GetTenantRequest{TenantId: "globex"}, scope: &id.Scope{Tenant: globex}},

		// Addressing errors.
		{name: "missing namespace reference", token: "read-star", method: mGetMemory, req: &memoryv1.GetMemoryRequest{},
			want: errs.ValidationReason("namespace", errs.ReasonRequired, "the namespace reference is required")},
		{name: "malformed namespace id", token: "read-star", method: mGetMemory,
			req:  &memoryv1.GetMemoryRequest{Namespace: &memoryv1.NamespaceRef{TenantId: "acme", NamespaceId: "nope"}},
			want: errs.ValidationReason("namespace.namespace_id", errs.ReasonBadFormat, "not a namespace id")},

		// N128: whatever the handler returns, no engram.internal.* detail reaches the client.
		{name: "internal MovedOutHint is dropped", token: "read-star", method: mGetMemory, req: getMemory(acme, a1),
			handlerErr: errs.MovedOut(a1, 7, 7, &errorsv1.MovedOutHint{NamespaceId: a1.String(), TargetShardId: 9,
				NextEpoch: 8}),
			scope: allowed(a1),
			want:  errs.WrongShardOrEpoch(a1, 7, 7, memoryv1.NamespaceState_NAMESPACE_STATE_MOVED_OUT)},
		{name: "FenceBusy renders as NamespaceFrozen", token: "write-a1", method: mRetain, req: retain(acme, a1),
			handlerErr: errs.FenceBusy(a1), scope: allowed(a1), want: errs.FenceBusy(a1)},
		{name: "a forwarded status keeps no internal detail", token: "read-star", method: mGetMemory,
			req: getMemory(acme, a1), scope: allowed(a1),
			handlerErr: errs.ToInternalStatus(
				errs.MovedOut(a1, 7, 7, &errorsv1.MovedOutHint{NamespaceId: a1.String()})).Err(),
			want: errs.WrongShardOrEpoch(a1, 7, 7, memoryv1.NamespaceState_NAMESPACE_STATE_MOVED_OUT)},
		{name: "an unknown handler error is redacted", token: "read-star", method: mGetMemory, req: getMemory(acme, a1),
			scope: allowed(a1), handlerErr: errors.New("pq: password authentication failed"),
			want: errs.Internal("x", nil)},
	}
}

// TestAuthz_Table is the authz table test of the M0.3 exit criterion: (claims, method, catalog entry) -> code, on the
// gRPC server and on the Connect server, with the error details compared byte for byte against the errs rendering of
// the expected error (whose goldens are testdata/errors/*.json) and against each other.
func TestAuthz_Table(t *testing.T) {
	for i, tc := range authzCases() {
		t.Run(tc.name, func(t *testing.T) {
			res := fixtureResolver()
			if tc.before != nil {
				tc.before(res)
			}
			ic := authz.NewInterceptor(fixtureVerifier(), res, tc.limiter, authz.DefaultPolicy()).With(
				authz.InterceptorOptions{Cells: tc.cells, Tenants: tenantFixture()})
			e := newEnv(t, ic)
			caseID := "c" + string(rune('A'+i/26)) + string(rune('a'+i%26))
			if tc.handlerErr != nil {
				e.probe.handlerErr[caseID] = tc.handlerErr
			}

			wantStatus := status.New(codes.Unimplemented, "reached the handler")
			if tc.want != nil {
				wantStatus = errs.ToStatus(tc.want)
			}
			want := fromStatus(wantStatus)
			if tc.want == nil {
				want.Message = "" // the transports word UNIMPLEMENTED differently; the code and the details decide
			}

			for _, tr := range []struct {
				name string
				call func(method, token, caseID string, req proto.Message) outcome
			}{{"grpc", e.callGRPC}, {"connect", e.callConnect}} {
				e.probe.clear(caseID)
				got := tr.call(tc.method, tc.token, caseID, tc.req)
				if tc.want == nil {
					got.Message = ""
				}
				if got.Code != want.Code || got.Message != want.Message || !equalStrings(got.Details, want.Details) {
					t.Errorf("%s: got %+v\n want %+v", tr.name, got, want)
				}
				if tc.golden != "" {
					if g := loadGolden(t, tc.golden); g.Code != got.Code || g.Message != got.Message ||
						!equalStrings(g.Details, got.Details) {
						t.Errorf("%s: got %+v\n golden %s = %+v", tr.name, got, tc.golden, g)
					}
				}
				r, reachedHandler := e.probe.reached(caseID)
				switch {
				case tc.scope != nil && !reachedHandler:
					t.Errorf("%s: the handler was not reached", tr.name)
				case tc.scope != nil:
					if !r.ok || r.scope.Scope != *tc.scope {
						t.Errorf("%s: RequestScope = %+v (ok %v); want %+v", tr.name, r.scope.Scope, r.ok, *tc.scope)
					}
				case reachedHandler:
					t.Errorf("%s: the handler was reached by a call that must be refused", tr.name)
				}
			}
		})
	}
}

// loadGolden reads testdata/errors/<name>.json (written by the errs tests) as an outcome.
func loadGolden(t *testing.T, name string) outcome {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "errors", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details []struct {
			Type string `json:"type"`
			Hex  string `json:"hex"`
		} `json:"details"`
	}
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	o := outcome{Message: g.Message}
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		if c.String() == g.Code {
			o.Code = c
		}
	}
	for _, d := range g.Details {
		o.Details = append(o.Details, d.Type+":"+d.Hex)
	}
	return o
}

func tenantDeleting() error {
	return errs.PreconditionFailed(errs.PreconditionTenantDeleting, "tdel", "the tenant is being deleted")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
