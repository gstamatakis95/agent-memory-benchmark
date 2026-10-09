package authz_test

import (
	"context"
	"sort"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"pgregory.net/rapid"

	adminv1 "github.com/gstamatakis95/engram/gen/go/memory/admin/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// servedRPCs lists every RPC of memory.v1 and memory.admin.v1 with its input message.
func servedRPCs() map[string]protoreflect.MessageDescriptor {
	out := map[string]protoreflect.MessageDescriptor{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		pkg := string(fd.Package())
		if pkg != "memory.v1" && pkg != "memory.admin.v1" {
			return true
		}
		for i := range fd.Services().Len() {
			s := fd.Services().Get(i)
			for j := range s.Methods().Len() {
				m := s.Methods().Get(j)
				out["/"+string(s.FullName())+"/"+string(m.Name())] = m.Input()
			}
		}
		return true
	})
	return out
}

// TestPolicy_CoversEveryRPC: the table is closed over the served protos (a new RPC cannot ship without a row, and a row
// cannot outlive its RPC), and each row's Target matches the shape of its request message.
func TestPolicy_CoversEveryRPC(t *testing.T) {
	rpcs, pol := servedRPCs(), authz.DefaultPolicy()
	if len(rpcs) < 50 {
		t.Fatalf("only %d RPCs found in the registry", len(rpcs))
	}
	for m, in := range rpcs {
		p, ok := pol[m]
		if !ok {
			t.Errorf("%s has no policy row", m)
			continue
		}
		hasRef := false
		if fd := in.Fields().ByName("namespace"); fd != nil && fd.Message() != nil &&
			fd.Message().FullName() == "memory.v1.NamespaceRef" {
			hasRef = true
		}
		hasTenantID := in.Fields().ByName("tenant_id") != nil
		switch p.Target {
		case authz.TargetNamespace:
			if !hasRef {
				t.Errorf("%s is namespace-scoped but %s has no NamespaceRef", m, in.Name())
			}
		case authz.TargetTenant:
			_, viaPatch := map[string]bool{"UpdateTenantRequest": true}[string(in.Name())]
			if !hasTenantID && !viaPatch {
				t.Errorf("%s is tenant-scoped but %s has no tenant_id", m, in.Name())
			}
		case authz.TargetOperator:
			if p.Scope != authz.ScopeOperator {
				t.Errorf("%s is operator-scoped with scope %s", m, p.Scope)
			}
		default:
			t.Errorf("%s: invalid target %d", m, p.Target)
		}
		if p.Scope == "" {
			t.Errorf("%s: no scope", m)
		}
	}
	for m := range pol {
		if _, ok := rpcs[m]; !ok {
			t.Errorf("policy row %s names no RPC", m)
		}
	}
}

// TestPolicy_MatchesTheScopeTable restates PLAN.md section 4.1.9 independently of policy.go.
func TestPolicy_MatchesTheScopeTable(t *testing.T) {
	want := map[authz.Scope][]string{
		authz.ScopeMemoryRead: {"MemoryService/Recall", "MemoryService/Reflect", "MemoryService/GetMemory",
			"MemoryService/ListMemories", "MemoryService/BatchGetMemories", "DocumentService/GetDocument",
			"DocumentService/ListDocuments", "DocumentService/GetDocumentVersion", "DocumentService/GetDocumentBody",
			"DocumentService/ListTags", "NamespaceService/GetNamespace", "NamespaceService/ListNamespaces",
			"NamespaceService/GetEffectiveConfig", "OperationService/GetOperation", "OperationService/ListOperations",
			"OperationService/WaitOperation", "ExportService/GetSnapshotManifest", "ExportService/ListSnapshots",
			"ExportService/StreamSnapshot", "PageService/GetPage", "PageService/ListPages", "PageService/SearchPages"},
		authz.ScopeMemoryWrite: {"MemoryService/Retain", "MemoryService/Invalidate", "MemoryService/Restore",
			"DocumentService/DeleteDocument", "DocumentService/UpdateDocumentTags", "ExportService/CreateSnapshot",
			"PageService/CreatePage", "PageService/UpdatePage", "PageService/DeletePage", "PageService/RefreshPage",
			"OperationService/CancelOperation"},
		authz.ScopeMemoryAdmin: {"NamespaceService/CreateNamespace", "NamespaceService/UpdateNamespace",
			"NamespaceService/DeleteNamespace"},
		authz.ScopeTenantAdmin: {"TenantService/GetTenant", "TenantService/DeleteTenant",
			"TenantService/GetTenantOperation", "TenantService/UpdateTenant"},
		authz.ScopeOperator: {"TenantService/ListTenants", "TenantService/CreateTenant",
			"TenantService/UpdateTenantLimits", "ShardService/RegisterShard", "ShardService/GetShard",
			"ShardService/ListShards", "ShardService/DrainShard", "ShardService/UpdateShard",
			"ShardService/ResolveNamespace", "ShardService/ReleaseNamespace", "MoveService/StartMove",
			"MoveService/GetMove", "MoveService/ListMoves", "MoveService/RollbackMove", "MoveService/CleanupMove"},
	}
	pol := authz.DefaultPolicy()
	got := map[authz.Scope][]string{}
	for m, p := range pol {
		short := m[len("/memory."):]
		short = short[indexAfter(short, '.'):] // drop "v1." or "admin.v1."
		got[p.Scope] = append(got[p.Scope], short)
	}
	for s, ms := range want {
		sort.Strings(ms)
		sort.Strings(got[s])
		if len(ms) != len(got[s]) {
			t.Errorf("scope %s: %d methods, want %d\n got: %v\nwant: %v", s, len(got[s]), len(ms), got[s], ms)
			continue
		}
		for i := range ms {
			if ms[i] != got[s][i] {
				t.Errorf("scope %s: method %d is %s, want %s", s, i, got[s][i], ms[i])
			}
		}
	}
	// The special rows.
	for _, m := range []string{"/memory.v1.OperationService/GetOperation", "/memory.v1.OperationService/WaitOperation",
		"/memory.admin.v1.TenantService/GetTenantOperation"} {
		if !pol[m].AllowDeleting {
			t.Errorf("%s must allow DELETING (N70)", m)
		}
	}
	for _, m := range []string{"/memory.admin.v1.ShardService/ReleaseNamespace",
		"/memory.admin.v1.MoveService/CleanupMove"} {
		if p := pol[m]; !p.CellBound || p.AltScope != authz.ScopeWorker {
			t.Errorf("%s: %+v; want cell-bound with the worker alt scope (N167)", m, p)
		}
	}
	for _, m := range []string{"GetTenant", "DeleteTenant", "GetTenantOperation"} {
		if pol["/memory.admin.v1.TenantService/"+m].AltScope != authz.ScopeOperator {
			t.Errorf("%s must accept engram.operator on the admin route", m)
		}
	}
	nAllowDel := 0
	for _, p := range pol {
		if p.AllowDeleting {
			nAllowDel++
		}
	}
	if nAllowDel != 3 {
		t.Errorf("%d methods allow DELETING; want exactly 3", nAllowDel)
	}
}

func indexAfter(s string, c byte) int {
	for i := range len(s) {
		if s[i] == c {
			if len(s) > i+3 && s[i+1:i+3] == "v1" {
				return i + 4
			}
			return i + 1
		}
	}
	return 0
}

// propMethod is a method with a builder of its request for a given request tenant and namespace.
type propMethod struct {
	name string
	req  func(t id.TenantID, ns id.NamespaceID) proto.Message
}

func propMethods() []propMethod {
	return []propMethod{
		{mGetMemory, getMemory},
		{mRetain, retain},
		{mGetOp, func(t id.TenantID, ns id.NamespaceID) proto.Message {
			return &memoryv1.GetOperationRequest{Namespace: ref(t, ns), OperationId: "o"}
		}},
		{mDeleteNS, func(t id.TenantID, ns id.NamespaceID) proto.Message {
			return &memoryv1.DeleteNamespaceRequest{Namespace: ref(t, ns)}
		}},
		{mListNS, func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &memoryv1.ListNamespacesRequest{TenantId: t.String()}
		}},
		{mCreateNS, func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &memoryv1.CreateNamespaceRequest{TenantId: t.String()}
		}},
		{mGetTenant, func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &adminv1.GetTenantRequest{TenantId: t.String()}
		}},
		{mDelTenant, func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &adminv1.DeleteTenantRequest{TenantId: t.String(), ConfirmTenantId: t.String()}
		}},
		{"/memory.admin.v1.TenantService/GetTenantOperation", func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &adminv1.GetTenantOperationRequest{TenantId: t.String()}
		}},
		{"/memory.admin.v1.TenantService/UpdateTenant", func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &adminv1.UpdateTenantRequest{Patch: &adminv1.Tenant{TenantId: t.String()}}
		}},
		{mListTenants, func(id.TenantID, id.NamespaceID) proto.Message { return &adminv1.ListTenantsRequest{} }},
		{mLimits, func(t id.TenantID, _ id.NamespaceID) proto.Message {
			return &adminv1.UpdateTenantLimitsRequest{Patch: &adminv1.Tenant{TenantId: t.String()}}
		}},
		{mStartMove, func(t id.TenantID, ns id.NamespaceID) proto.Message {
			return &adminv1.StartMoveRequest{Namespace: ref(t, ns)}
		}},
		{mRelease, func(t id.TenantID, ns id.NamespaceID) proto.Message {
			return &adminv1.ReleaseNamespaceRequest{Namespace: ref(t, ns)}
		}},
		{mCleanup, func(id.TenantID, id.NamespaceID) proto.Message { return &adminv1.CleanupMoveRequest{MoveId: "m"} }},
	}
}

// TestAuthz_Properties are the two properties of PLAN.md section 8.2 (internal/authz row), over random claims (any of
// the six scopes, a tenant or none, an allowlist, a cell), every kind of method (namespace-scoped, tenant-level,
// operator, cell-bound), request tenants drawn independently of the token's, and the catalog entries of the fixture,
// driven through the real unary interceptor.
//
//	Allowlist:  ns = ["*"] admits every namespace of the same tenant and none of another; a call that passes never
//	            has a RequestScope whose tenant differs from the token's (unless the token is a fleet token).
//	Monotone:   adding a scope to a token never turns an allowed call into a denied one.
func TestAuthz_Properties(t *testing.T) {
	allScopes := []authz.Scope{authz.ScopeMemoryRead, authz.ScopeMemoryWrite, authz.ScopeMemoryAdmin,
		authz.ScopeTenantAdmin, authz.ScopeOperator, authz.ScopeWorker}
	methods := propMethods()
	nss := []id.NamespaceID{a1, a2, aDeleting, aGone, g1, aTenantDel, ghost}
	res := fixtureResolver()
	tenants := tenantFixture()
	cells := func(context.Context, string, proto.Message) (id.CellID, error) { return "cell-a", nil }

	run := func(c *authz.Claims, pmeth propMethod, reqTenant id.TenantID,
		ns id.NamespaceID) (authz.RequestScope, error) {
		v := mapVerifier{claims: map[string]*authz.Claims{"t": c}}
		ic := authz.NewInterceptor(v, res, nil, authz.DefaultPolicy()).With(authz.InterceptorOptions{Cells: cells,
			Tenants: tenants})
		var rs authz.RequestScope
		var ok bool
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer t"))
		_, err := ic.Unary()(ctx, pmeth.req(reqTenant, ns), &grpc.UnaryServerInfo{FullMethod: pmeth.name},
			func(ctx context.Context, _ any) (any, error) { rs, ok = authz.FromContext(ctx); return nil, nil })
		if err == nil && !ok {
			return rs, errs.Internal("handler ran without a RequestScope", nil)
		}
		if err != nil {
			return rs, errs.FromStatus(status.Convert(err)) // what a client sees, back as an *errs.Error
		}
		return rs, nil
	}
	rapid.Check(t, func(rt *rapid.T) {
		tenant := rapid.SampledFrom([]id.TenantID{acme, globex, ""}).Draw(rt, "token tenant")
		reqTenant := rapid.SampledFrom([]id.TenantID{acme, globex}).Draw(rt, "request tenant")
		scopeKey := func(s authz.Scope) string { return string(s) }
		have := rapid.SliceOfNDistinct(rapid.SampledFrom(allScopes), 0, 6, scopeKey).Draw(rt, "scopes")
		pmeth := rapid.SampledFrom(methods).Draw(rt, "method")
		ns := rapid.SampledFrom(nss).Draw(rt, "ns")
		star := rapid.Bool().Draw(rt, "star")
		cell := rapid.SampledFrom([]id.CellID{"cell-a", "cell-b"}).Draw(rt, "cell")
		c := &authz.Claims{Subject: "u", Tenant: tenant, Scopes: have, AllNamespaces: star, Cell: cell}

		rs, err := run(c, pmeth, reqTenant, ns)
		fleet := contains(have, authz.ScopeOperator) || contains(have, authz.ScopeWorker)
		if err == nil && rs.Tenant != "" && rs.Tenant != tenant && !fleet {
			rt.Fatalf("a call passed with RequestScope tenant %q for a token of %q", rs.Tenant, tenant)
		}
		// Allowlist: with "*" and the right scope, same-tenant live namespaces pass, another tenant's never do.
		e, _ := res.Resolve(context.Background(), ns)
		if star && pmeth.name == mGetMemory && contains(have, authz.ScopeMemoryRead) && tenant != "" &&
			reqTenant == tenant {
			switch {
			case e == nil, e.Tenant != tenant, e.State == catalog.StateDeleted:
				if !errs.Is(err, errs.KindNotFound) {
					rt.Fatalf("ns %v of another tenant or absent: err = %v; want NOT_FOUND", ns, err)
				}
			case e.State == catalog.StateActive && e.TenantEntry.State == "active":
				if err != nil {
					rt.Fatalf("star + memory.read on a live namespace of the same tenant: %v", err)
				}
			}
		}
		// Monotonicity: every additional scope keeps an allowed call allowed.
		if err == nil {
			for _, extra := range allScopes {
				more := *c
				more.Scopes = append(append([]authz.Scope{}, have...), extra)
				if _, err2 := run(&more, pmeth, reqTenant, ns); err2 != nil {
					rt.Fatalf("adding %s turned an allowed %s into %v", extra, pmeth.name, err2)
				}
			}
		}
	})
}

func contains(ss []authz.Scope, s authz.Scope) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
