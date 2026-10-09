package authz_test

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"

	adminv1 "github.com/gstamatakis95/engram/gen/go/memory/admin/v1"
	"github.com/gstamatakis95/engram/gen/go/memory/admin/v1/adminv1connect"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/gen/go/memory/v1/memoryv1connect"
	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/quota"
)

// mapVerifier maps a bearer token to claims or to an error; the empty token is "missing".
type mapVerifier struct {
	claims map[string]*authz.Claims
	failed map[string]error
}

func (m mapVerifier) Verify(_ context.Context, raw string) (*authz.Claims, error) {
	if raw == "" {
		return nil, errs.Unauthenticated("missing bearer token")
	}
	if err, ok := m.failed[raw]; ok {
		return nil, err
	}
	if c, ok := m.claims[raw]; ok {
		return c, nil
	}
	return nil, errs.Unauthenticated("invalid token")
}

// denyLimiter refuses the buckets it names.
type denyLimiter struct {
	deny map[quota.Bucket]time.Duration
}

func (d denyLimiter) Allow(_ context.Context, k quota.Key, _ int64) (quota.Decision, error) {
	if r, ok := d.deny[k.Bucket]; ok {
		return quota.Decision{Allowed: false, RetryAfter: r}, nil
	}
	return quota.Decision{Allowed: true}, nil
}

// reached is what the handler end of the chain saw.
type reached struct {
	scope authz.RequestScope
	ok    bool
}

// probe is the innermost handler on both transports: it records the RequestScope the interceptor attached and answers
// with the case's handler error, or UNIMPLEMENTED ("the interceptor let the call through").
type probe struct {
	mu         sync.Mutex
	got        map[string]reached
	handlerErr map[string]error
}

func newProbe() *probe { return &probe{got: map[string]reached{}, handlerErr: map[string]error{}} }

func (p *probe) record(ctx context.Context, caseID string) error {
	rs, ok := authz.FromContext(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got[caseID] = reached{rs, ok}
	if err := p.handlerErr[caseID]; err != nil {
		return err
	}
	return errUnimplemented
}

var errUnimplemented = status.Error(codes.Unimplemented, "reached the handler")

func (p *probe) reached(caseID string) (reached, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.got[caseID]
	return r, ok
}

func (p *probe) clear(caseID string) {
	p.mu.Lock()
	delete(p.got, caseID)
	p.mu.Unlock()
}

func caseOf(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("x-case"); len(v) > 0 {
		return v[0]
	}
	return ""
}

func inputOf(fullMethod string) (protoreflect.MessageDescriptor, bool) {
	var svc, method string
	for i := len(fullMethod) - 1; i > 0; i-- {
		if fullMethod[i] == '/' {
			svc, method = fullMethod[1:i], fullMethod[i+1:]
			break
		}
	}
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svc))
	if err != nil {
		return nil, false
	}
	m := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(method))
	if m == nil {
		return nil, false
	}
	return m.Input(), m.IsStreamingServer()
}

func (p *probe) grpcUnary(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
	return nil, p.record(ctx, caseOf(ctx))
}

func (p *probe) grpcStream(_ any, ss grpc.ServerStream, info *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
	in, _ := inputOf(info.FullMethod)
	if err := ss.RecvMsg(dynamicpb.NewMessage(in)); err != nil {
		return err
	}
	return p.record(ss.Context(), caseOf(ss.Context()))
}

// connectProbe records on unary Connect calls; the three server-streaming methods are overridden below because the
// generated stream wrapper reads the request before it calls the implementation.
type connectProbe struct{ p *probe }

func (c connectProbe) WrapUnary(connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, c.p.recordConnect(ctx, req.Header().Get("x-case"))
	}
}
func (connectProbe) WrapStreamingClient(n connect.StreamingClientFunc) connect.StreamingClientFunc {
	return n
}
func (connectProbe) WrapStreamingHandler(n connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return n
}

// recordConnect is record with a Connect error: the probe's gRPC status error is converted at the edge.
func (p *probe) recordConnect(ctx context.Context, caseID string) error {
	err := p.record(ctx, caseID)
	if errors.Is(err, errUnimplemented) {
		return connect.NewError(connect.CodeUnimplemented, errors.New("reached the handler"))
	}
	return err
}

type memoryStreams struct {
	memoryv1connect.UnimplementedMemoryServiceHandler
	p *probe
}

func (m memoryStreams) Recall(ctx context.Context, r *connect.Request[memoryv1.RecallRequest],
	_ *connect.ServerStream[memoryv1.RecallResponse]) error {
	return m.p.recordConnect(ctx, r.Header().Get("x-case"))
}

func (m memoryStreams) Reflect(ctx context.Context, r *connect.Request[memoryv1.ReflectRequest],
	_ *connect.ServerStream[memoryv1.ReflectResponse]) error {
	return m.p.recordConnect(ctx, r.Header().Get("x-case"))
}

type exportStreams struct {
	memoryv1connect.UnimplementedExportServiceHandler
	p *probe
}

func (e exportStreams) StreamSnapshot(ctx context.Context, r *connect.Request[memoryv1.StreamSnapshotRequest],
	_ *connect.ServerStream[memoryv1.StreamSnapshotResponse]) error {
	return e.p.recordConnect(ctx, r.Header().Get("x-case"))
}

// env serves one interceptor on a gRPC server (bufconn) and a Connect server (httptest), side by side.
type env struct {
	ic      *authz.Interceptor
	probe   *probe
	conn    *grpc.ClientConn
	baseURL string
	http    *http.Client
}

func newEnv(t testing.TB, ic *authz.Interceptor) *env {
	t.Helper()
	pr := newProbe()
	e := &env{ic: ic, probe: pr}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(ic.Unary(), pr.grpcUnary),
		grpc.ChainStreamInterceptor(ic.Stream(), pr.grpcStream))
	memoryv1.RegisterMemoryServiceServer(gs, memoryv1.UnimplementedMemoryServiceServer{})
	memoryv1.RegisterDocumentServiceServer(gs, memoryv1.UnimplementedDocumentServiceServer{})
	memoryv1.RegisterNamespaceServiceServer(gs, memoryv1.UnimplementedNamespaceServiceServer{})
	memoryv1.RegisterOperationServiceServer(gs, memoryv1.UnimplementedOperationServiceServer{})
	memoryv1.RegisterPageServiceServer(gs, memoryv1.UnimplementedPageServiceServer{})
	memoryv1.RegisterExportServiceServer(gs, memoryv1.UnimplementedExportServiceServer{})
	adminv1.RegisterTenantServiceServer(gs, adminv1.UnimplementedTenantServiceServer{})
	adminv1.RegisterShardServiceServer(gs, adminv1.UnimplementedShardServiceServer{})
	adminv1.RegisterMoveServiceServer(gs, adminv1.UnimplementedMoveServiceServer{})
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(
		func(context.Context, string) (net.Conn, error) { return lis.DialContext(context.Background()) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	e.conn = conn

	mux := http.NewServeMux()
	opts := connect.WithInterceptors(ic.Connect(), connectProbe{pr})
	for _, mount := range []func() (string, http.Handler){
		func() (string, http.Handler) {
			return memoryv1connect.NewMemoryServiceHandler(memoryStreams{p: pr}, opts)
		},
		func() (string, http.Handler) {
			h := memoryv1connect.UnimplementedDocumentServiceHandler{}
			return memoryv1connect.NewDocumentServiceHandler(h, opts)
		},
		func() (string, http.Handler) {
			h := memoryv1connect.UnimplementedNamespaceServiceHandler{}
			return memoryv1connect.NewNamespaceServiceHandler(h, opts)
		},
		func() (string, http.Handler) {
			h := memoryv1connect.UnimplementedOperationServiceHandler{}
			return memoryv1connect.NewOperationServiceHandler(h, opts)
		},
		func() (string, http.Handler) {
			return memoryv1connect.NewPageServiceHandler(memoryv1connect.UnimplementedPageServiceHandler{}, opts)
		},
		func() (string, http.Handler) {
			return memoryv1connect.NewExportServiceHandler(exportStreams{p: pr}, opts)
		},
		func() (string, http.Handler) {
			return adminv1connect.NewTenantServiceHandler(adminv1connect.UnimplementedTenantServiceHandler{}, opts)
		},
		func() (string, http.Handler) {
			return adminv1connect.NewShardServiceHandler(adminv1connect.UnimplementedShardServiceHandler{}, opts)
		},
		func() (string, http.Handler) {
			return adminv1connect.NewMoveServiceHandler(adminv1connect.UnimplementedMoveServiceHandler{}, opts)
		},
	} {
		path, h := mount()
		mux.Handle(path, h)
	}
	ts := httptest.NewServer(mux)
	e.baseURL, e.http = ts.URL, ts.Client()
	t.Cleanup(func() { _ = conn.Close(); gs.Stop(); ts.Close() })
	return e
}

// outcome is a transport-independent rendering of a call result: gRPC code, message and the exact bytes of each detail.
type outcome struct {
	Code    codes.Code
	Message string
	Details []string
}

func fromStatus(st *status.Status) outcome {
	o := outcome{Code: st.Code(), Message: st.Message()}
	for _, a := range st.Proto().GetDetails() {
		o.Details = append(o.Details, string(a.MessageName())+":"+hex.EncodeToString(a.GetValue()))
	}
	return o
}

func fromConnectErr(err error) outcome {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return outcome{Code: codes.Unknown, Message: err.Error()}
	}
	o := outcome{Code: codes.Code(ce.Code()), Message: ce.Message()} //nolint:gosec // equal code spaces
	for _, d := range ce.Details() {
		o.Details = append(o.Details, d.Type()+":"+hex.EncodeToString(d.Bytes()))
	}
	return o
}

// callGRPC invokes method with req over the gRPC server and returns what the client saw.
func (e *env) callGRPC(method, token, caseID string, req proto.Message) outcome {
	md := metadata.Pairs("x-case", caseID)
	if token != "" {
		md.Set("authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 10*time.Second)
	defer cancel()
	_, streaming := inputOf(method)
	var err error
	if streaming {
		var s grpc.ClientStream
		if s, err = e.conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method); err == nil {
			if err = s.SendMsg(req); err == nil {
				_ = s.CloseSend()
				err = s.RecvMsg(&emptypb.Empty{})
			}
		}
	} else {
		err = e.conn.Invoke(ctx, method, req, &emptypb.Empty{})
	}
	st, _ := status.FromError(err)
	return fromStatus(st)
}

// callConnect invokes method with req over the Connect server.
func (e *env) callConnect(method, token, caseID string, req proto.Message) outcome {
	dyn := dynamicpb.NewMessage(req.ProtoReflect().Descriptor())
	b, _ := proto.Marshal(req)
	_ = proto.Unmarshal(b, dyn)
	client := connect.NewClient[dynamicpb.Message, emptypb.Empty](e.http, e.baseURL+method)
	r := connect.NewRequest(dyn)
	r.Header().Set("x-case", caseID)
	if token != "" {
		r.Header().Set("Authorization", "Bearer "+token)
	}
	_, streaming := inputOf(method)
	var err error
	if streaming {
		var s *connect.ServerStreamForClient[emptypb.Empty]
		if s, err = client.CallServerStream(context.Background(), r); err == nil {
			for s.Receive() {
			}
			err = s.Err()
		}
	} else {
		_, err = client.CallUnary(context.Background(), r)
	}
	return fromConnectErr(err)
}

// ids of the fixture. All are valid UUIDv7.
func nsID(last byte) id.NamespaceID {
	n, err := id.ParseNamespaceID("018f0000-0000-7000-8000-0000000000" + hex.EncodeToString([]byte{last}))
	if err != nil {
		panic(err)
	}
	return n
}

func ref(t id.TenantID, ns id.NamespaceID) *memoryv1.NamespaceRef {
	return &memoryv1.NamespaceRef{TenantId: t.String(), NamespaceId: ns.String()}
}
