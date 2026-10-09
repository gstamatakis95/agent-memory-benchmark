package authz

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/config"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/quota"
)

// CellLookup returns the cell that serves the shard a cell-bound call (ReleaseNamespace, CleanupMove) acts on. The
// interceptor compares it with the `cell` claim of an engram.worker token (N167). Without a lookup, a worker token is
// refused on those methods: the binding cannot be verified, so it fails closed.
type CellLookup func(ctx context.Context, method string, req proto.Message) (id.CellID, error)

// ConfigFunc resolves the effective configuration of a catalog entry for RequestScope.Config (config.Resolver over the
// system, tenant and namespace layers). Without one, RequestScope.Config is nil.
type ConfigFunc func(e *catalog.Entry) (*config.Resolved, error)

// TenantLookup returns the state of a tenant (active | suspended | deleting | deleted), cached by the caller
// (catalog.TenantStates.State). With one, a tenant-level method on a `deleting` tenant is refused with
// PreconditionFailed{TENANT_DELETING} unless the method's policy has AllowDeleting (N5, N122, N70); without one the
// tenant barrier is enforced only through the catalog entries of namespace-scoped methods.
type TenantLookup func(ctx context.Context, t id.TenantID) (string, error)

// InterceptorOptions are the optional parts of an Interceptor.
type InterceptorOptions struct {
	// Public lists full method names served without authentication or policy (gRPC health and reflection). A method
	// that is neither in the Policy nor here is refused: the table is closed.
	Public  []string
	Cells   CellLookup
	Config  ConfigFunc
	Tenants TenantLookup
}

// Interceptor is the one enforcement point; it holds the verifier, the resolver, the limiter and the policy table.
type Interceptor struct {
	verifier TokenVerifier
	resolver catalog.Resolver
	limiter  quota.Limiter
	policy   Policy
	public   map[string]bool
	cells    CellLookup
	config   ConfigFunc
	tenants  TenantLookup
}

// NewInterceptor wires the interceptor. limiter may be nil (no rate limiting).
func NewInterceptor(v TokenVerifier, r catalog.Resolver, l quota.Limiter, p Policy) *Interceptor {
	return &Interceptor{verifier: v, resolver: r, limiter: l, policy: p, public: map[string]bool{}}
}

// With applies the optional parts and returns the interceptor.
func (i *Interceptor) With(o InterceptorOptions) *Interceptor {
	for _, m := range o.Public {
		i.public[m] = true
	}
	i.cells, i.config, i.tenants = o.Cells, o.Config, o.Tenants
	return i
}

// ctxKey is the type of the context keys of this package.
type ctxKey uint8

const (
	scopeKey ctxKey = iota + 1
	headerKey
)

// scopeCell holds the RequestScope of a stream: the context of a stream handler is created before the request message
// arrives, so the scope is stored once, at the first message, and read from the same cell afterwards (N8).
type scopeCell struct {
	mu    sync.RWMutex
	scope RequestScope
	set   bool
}

// FromContext returns the RequestScope the interceptor attached.
func FromContext(ctx context.Context) (RequestScope, bool) {
	switch v := ctx.Value(scopeKey).(type) {
	case RequestScope:
		return v, true
	case *scopeCell:
		v.mu.RLock()
		defer v.mu.RUnlock()
		return v.scope, v.set
	}
	return RequestScope{}, false
}

// HeaderFromContext returns a reader of the request headers (gRPC metadata or HTTP headers) of the call, or nil. It
// lets a TokenVerifier that needs more than the bearer token (AllowAllVerifier) read its input.
func HeaderFromContext(ctx context.Context) func(string) string {
	f, _ := ctx.Value(headerKey).(func(string) string)
	return f
}

func metadataHeader(md metadata.MD) func(string) string {
	return func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
}

func bearer(h func(string) string) string {
	v := strings.TrimSpace(h("authorization"))
	if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
		return strings.TrimSpace(v[7:])
	}
	return ""
}

func hasScope(c *Claims, s Scope) bool {
	return s != "" && slices.Contains(c.Scopes, s)
}

func (c *Claims) allows(ns id.NamespaceID, group string) bool {
	if c.AllNamespaces || slices.Contains(c.Namespaces, ns) {
		return true
	}
	return group != "" && slices.Contains(c.Groups, group)
}

// authorize runs the checks of PLAN.md section 1.3 steps 2 to 7 for one call and returns the scope for the handler.
// msg is the request message. The order is the contract (4.1.1): token, then the addressed tenant (NOT_FOUND for
// another tenant, never revealing the namespace), the catalog entry, the allowlist, the scope, the `deleting` state,
// the cell binding, and the rate bucket.
func (i *Interceptor) authorize(ctx context.Context, method string, hdr func(string) string,
	msg proto.Message) (RequestScope, error) {
	pol, ok := i.policy[method]
	if !ok {
		return RequestScope{}, errs.Wrap(errs.KindPermissionDenied, "method "+method+" is not in the policy", nil)
	}
	vctx := context.WithValue(ctx, headerKey, hdr)
	claims, err := i.verifier.Verify(vctx, bearer(hdr))
	if err != nil {
		if errs.KindOf(err) != 0 {
			return RequestScope{}, err
		}
		return RequestScope{}, errs.Unauthenticated("invalid token")
	}
	rs := RequestScope{Caller: id.Caller{Subject: claims.Subject}, Scopes: claims.Scopes}
	addr, err := addressOf(msg, pol.Target == TargetNamespace)
	if err != nil {
		return RequestScope{}, err
	}
	rs.Caller.RequestID = addr.requestID

	switch pol.Target {
	case TargetOperator:
		if err := requireScope(claims, pol); err != nil {
			return RequestScope{}, err
		}
		if err := i.checkCell(ctx, method, msg, claims, pol); err != nil {
			return RequestScope{}, err
		}
		return rs, nil
	case TargetTenant:
		// The operator authority on the admin route carries no tenant, so the tenant-bound check is skipped whenever
		// the token holds it, whatever else it holds: adding a scope never turns an allowed call into a denied one
		// (the scope monotonicity of PLAN.md section 8.2).
		if !hasScope(claims, pol.AltScope) {
			if !addr.tenantPresent || claims.Tenant == "" || addr.tenant != claims.Tenant {
				return RequestScope{}, errs.NotFoundTenant(addr.tenant)
			}
		}
		if err := requireScope(claims, pol); err != nil {
			return RequestScope{}, err
		}
		if err := i.checkTenantState(ctx, pol, addr.tenant); err != nil {
			return RequestScope{}, err
		}
		rs.Scope = id.Scope{Tenant: addr.tenant}
		return rs, nil
	}
	return i.authorizeNamespace(ctx, claims, pol, addr, rs)
}

// checkTenantState refuses a tenant-level call on a tenant that is being deleted (TENANT_DELETING) or gone (NOT_FOUND),
// except on a method with AllowDeleting (GetTenantOperation, so that the DELETE_TENANT operation can be awaited).
func (i *Interceptor) checkTenantState(ctx context.Context, pol MethodPolicy, t id.TenantID) error {
	if i.tenants == nil || pol.AllowDeleting {
		return nil
	}
	st, err := i.tenants(ctx, t)
	if err != nil {
		if errs.Is(err, errs.KindNotFound) {
			return errs.NotFoundTenant(t)
		}
		return err
	}
	switch st {
	case "deleting":
		return errs.PreconditionFailed(errs.PreconditionTenantDeleting, t.String(), "the tenant is being deleted")
	case "deleted":
		return errs.NotFoundTenant(t)
	}
	return nil
}

func requireScope(c *Claims, p MethodPolicy) error {
	if hasScope(c, p.Scope) || hasScope(c, p.AltScope) {
		return nil
	}
	return errs.PermissionDenied(string(p.Scope))
}

func (i *Interceptor) checkCell(ctx context.Context, method string, msg proto.Message, c *Claims,
	p MethodPolicy) error {
	if !p.CellBound || !hasScope(c, ScopeWorker) || hasScope(c, p.Scope) {
		return nil // only an engram.worker token is bound to a cell; the operator is fleet-wide
	}
	if i.cells == nil {
		return errs.Wrap(errs.KindPermissionDenied, "the cell binding of the worker token cannot be verified", nil)
	}
	cell, err := i.cells(ctx, method, msg)
	if err != nil {
		return err
	}
	if cell == "" || cell != c.Cell {
		return errs.WrongCell(string(c.Cell))
	}
	return nil
}

func (i *Interceptor) authorizeNamespace(ctx context.Context, claims *Claims, pol MethodPolicy, addr addressing,
	rs RequestScope) (RequestScope, error) {
	gone := errs.NotFoundNamespace(addr.tenant, addr.namespace)
	if claims.Tenant == "" || addr.tenant != claims.Tenant {
		return RequestScope{}, gone // another tenant's namespace is indistinguishable from a missing one
	}
	e, err := i.resolver.Resolve(ctx, addr.namespace)
	if err != nil {
		if errs.Is(err, errs.KindNotFound) {
			return RequestScope{}, gone
		}
		return RequestScope{}, err
	}
	if e.Tenant != claims.Tenant {
		// While the catalog is down the cache answers for entries it holds; a cached entry of another tenant must not
		// answer differently from a miss, or the outage becomes an existence oracle (N5).
		if d, ok := i.resolver.(interface{ Down() bool }); ok && d.Down() {
			return RequestScope{}, errs.Unavailable("catalog unavailable", 2*time.Second)
		}
		return RequestScope{}, gone
	}
	if e.State == catalog.StateDeleted {
		return RequestScope{}, gone
	}
	if !claims.allows(e.Namespace, e.Group) {
		return RequestScope{}, errs.NamespaceNotAllowed(e.Namespace)
	}
	if err := requireScope(claims, pol); err != nil {
		return RequestScope{}, err
	}
	if !pol.AllowDeleting {
		if e.State == catalog.StateDeleting {
			return RequestScope{}, errs.PreconditionFailed(errs.PreconditionNamespaceDeleting, e.Namespace.String(),
				"the namespace is being deleted")
		}
		if e.TenantEntry != nil && e.TenantEntry.State == "deleting" {
			return RequestScope{}, errs.PreconditionFailed(errs.PreconditionTenantDeleting, e.Tenant.String(),
				"the tenant is being deleted")
		}
	}
	rs.Scope = id.Scope{Tenant: e.Tenant, Namespace: e.Namespace, Shard: e.Shard, Epoch: e.Epoch}
	if pol.Bucket != quota.BucketNone && i.limiter != nil {
		// D13: the bucket belongs to (tenant, namespace); shard and epoch change with a move or a restore.
		key := quota.Key{Scope: id.Scope{Tenant: rs.Tenant, Namespace: rs.Namespace}, Bucket: pol.Bucket}
		d, err := i.limiter.Allow(ctx, key, 1)
		if err != nil {
			return RequestScope{}, err
		}
		if !d.Allowed {
			return RequestScope{}, errs.QuotaExceeded(string(pol.Bucket), memoryv1.QuotaScope_QUOTA_SCOPE_NAMESPACE,
				0, 0, d.RetryAfter)
		}
	}
	if i.config != nil {
		cfg, err := i.config(e)
		if err != nil {
			return RequestScope{}, err
		}
		rs.Config = cfg
	}
	return rs, nil
}

// withScope returns ctx carrying the scope for the handler.
func withScope(ctx context.Context, rs RequestScope) context.Context {
	return context.WithValue(ctx, scopeKey, rs)
}

// Unary returns the gRPC unary interceptor.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if i.public[info.FullMethod] {
			return h(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		m, _ := req.(proto.Message)
		rs, err := i.authorize(ctx, info.FullMethod, metadataHeader(md), m)
		if err != nil {
			return nil, errs.ToStatus(err).Err()
		}
		resp, err := h(withScope(ctx, rs), req)
		if err != nil {
			return nil, errs.ToStatus(err).Err() // drops every engram.internal.* detail (N128)
		}
		return resp, nil
	}
}

// authStream wraps a gRPC server stream: the request message is read by the handler through RecvMsg, so the checks run
// on the first message, and the scope they produce is fixed for the stream's life (N8): a token that expires mid-stream
// does not abort a Reflect that runs 300 s.
type authStream struct {
	grpc.ServerStream
	ctx    context.Context
	cell   *scopeCell
	i      *Interceptor
	method string
	hdr    func(string) string
	once   sync.Once
	err    error
}

func (s *authStream) Context() context.Context { return s.ctx }

func (s *authStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	s.once.Do(func() {
		pm, _ := m.(proto.Message)
		rs, err := s.i.authorize(s.ctx, s.method, s.hdr, pm)
		if err != nil {
			s.err = errs.ToStatus(err).Err()
			return
		}
		s.cell.mu.Lock()
		s.cell.scope, s.cell.set = rs, true
		s.cell.mu.Unlock()
	})
	return s.err
}

// Stream returns the gRPC stream interceptor; the scope is fixed for the stream's life (N8).
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if i.public[info.FullMethod] {
			return h(srv, ss)
		}
		md, _ := metadata.FromIncomingContext(ss.Context())
		cell := &scopeCell{}
		as := &authStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), scopeKey, cell), cell: cell, i: i,
			method: info.FullMethod, hdr: metadataHeader(md)}
		if err := h(srv, as); err != nil {
			return errs.ToStatus(err).Err()
		}
		return nil
	}
}

// Connect returns the Connect interceptor.
func (i *Interceptor) Connect() connect.Interceptor { return connectInterceptor{i} }

type connectInterceptor struct{ i *Interceptor }

func httpHeader(h http.Header) func(string) string { return h.Get }

func (c connectInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if c.i.public[req.Spec().Procedure] {
			return next(ctx, req)
		}
		m, _ := req.Any().(proto.Message)
		rs, err := c.i.authorize(ctx, req.Spec().Procedure, httpHeader(req.Header()), m)
		if err != nil {
			return nil, errs.ToConnect(err)
		}
		resp, err := next(withScope(ctx, rs), req)
		if err != nil {
			return nil, errs.ToConnect(err)
		}
		return resp, nil
	}
}

func (c connectInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next // a server-side interceptor
}

// connStream wraps a Connect streaming handler connection; see authStream.
type connStream struct {
	connect.StreamingHandlerConn
	ctx  context.Context
	cell *scopeCell
	i    *Interceptor
	once sync.Once
	err  error
}

func (s *connStream) Receive(m any) error {
	if err := s.StreamingHandlerConn.Receive(m); err != nil {
		return err
	}
	s.once.Do(func() {
		pm, _ := m.(proto.Message)
		rs, err := s.i.authorize(s.ctx, s.Spec().Procedure, httpHeader(s.RequestHeader()), pm)
		if err != nil {
			s.err = errs.ToConnect(err)
			return
		}
		s.cell.mu.Lock()
		s.cell.scope, s.cell.set = rs, true
		s.cell.mu.Unlock()
	})
	return s.err
}

func (c connectInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if c.i.public[conn.Spec().Procedure] {
			return next(ctx, conn)
		}
		cell := &scopeCell{}
		ctx = context.WithValue(ctx, scopeKey, cell)
		if err := next(ctx, &connStream{StreamingHandlerConn: conn, ctx: ctx, cell: cell, i: c.i}); err != nil {
			return errs.ToConnect(err)
		}
		return nil
	}
}
