package authz_test

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/errs"
)

// fakeStream is a server stream that delivers one request message, then io.EOF-free repeats of it.
type fakeStream struct {
	grpc.ServerStream
	ctx  context.Context
	req  proto.Message
	recv int
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) RecvMsg(m any) error {
	f.recv++
	proto.Merge(m.(proto.Message), f.req)
	return nil
}

// countingVerifier verifies by counting; after the first call it reports the token as expired, as a clock that moved
// past `exp` mid-stream would.
type countingVerifier struct {
	calls atomic.Int32
	claim *authz.Claims
}

func (c *countingVerifier) Verify(context.Context, string) (*authz.Claims, error) {
	if c.calls.Add(1) > 1 {
		return nil, errs.Unauthenticated("token expired")
	}
	return c.claim, nil
}

// TestStream_ScopeIsFixedAtOpen is N8: the token is verified and the scope computed once, at the first message; a token
// that expires while the stream runs (Reflect may run 300 s) neither aborts it nor changes the scope.
func TestStream_ScopeIsFixedAtOpen(t *testing.T) {
	v := &countingVerifier{claim: claims(acme, authz.ScopeMemoryRead)}
	v.claim.AllNamespaces = true
	ic := authz.NewInterceptor(v, fixtureResolver(), nil, authz.DefaultPolicy())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer t"))
	ss := &fakeStream{ctx: ctx, req: recall(acme, a1)}

	var first authz.RequestScope
	err := ic.Stream()(nil, ss, &grpc.StreamServerInfo{FullMethod: mRecall, IsServerStream: true},
		func(_ any, s grpc.ServerStream) error {
			if _, ok := authz.FromContext(s.Context()); ok {
				t.Error("no scope is available before the request message is read")
			}
			if err := s.RecvMsg(&memoryv1.RecallRequest{}); err != nil {
				return err
			}
			var ok bool
			if first, ok = authz.FromContext(s.Context()); !ok {
				t.Fatal("no scope after the first message")
			}
			// Later messages (and the passage of time) neither re-verify nor change anything.
			for range 5 {
				if err := s.RecvMsg(&memoryv1.RecallRequest{}); err != nil {
					return err
				}
			}
			if again, _ := authz.FromContext(s.Context()); again.Scope != first.Scope {
				t.Error("the scope changed during the stream")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if v.calls.Load() != 1 {
		t.Errorf("verifier called %d times for one stream; want exactly 1", v.calls.Load())
	}
	if first.Tenant != acme || first.Namespace != a1 || first.Shard != 3 || first.Epoch != 7 {
		t.Errorf("scope = %+v", first.Scope)
	}
}

func TestStream_RefusedAtTheFirstMessage(t *testing.T) {
	ic := authz.NewInterceptor(mapVerifier{claims: map[string]*authz.Claims{"t": claims(acme, authz.ScopeMemoryRead)}},
		fixtureResolver(), nil, authz.DefaultPolicy())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer t"))
	ss := &fakeStream{ctx: ctx, req: recall(acme, g1)}
	err := ic.Stream()(nil, ss, &grpc.StreamServerInfo{FullMethod: mRecall, IsServerStream: true},
		func(_ any, s grpc.ServerStream) error { return s.RecvMsg(&memoryv1.RecallRequest{}) })
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v; want NOT_FOUND", err)
	}
	if status.Convert(err).Message() != errs.ToStatus(errs.NotFoundNamespace(acme, g1)).Message() {
		t.Errorf("message = %q", status.Convert(err).Message())
	}
}

func TestInterceptor_ClosedPolicyAndPublicMethods(t *testing.T) {
	ic := authz.NewInterceptor(mapVerifier{}, fixtureResolver(), nil, authz.DefaultPolicy()).With(
		authz.InterceptorOptions{Public: []string{"/grpc.health.v1.Health/Check"}})
	called := false
	h := func(context.Context, any) (any, error) { called = true; return "ok", nil }
	ctx := context.Background()

	_, err := ic.Unary()(ctx, getMemory(acme, a1), &grpc.UnaryServerInfo{FullMethod: "/x.Y/Unlisted"}, h)
	if status.Code(err) != codes.PermissionDenied || called {
		t.Errorf("a method outside the table = %v (handler called: %v); want PERMISSION_DENIED", err, called)
	}
	resp, err := ic.Unary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, h)
	if err != nil || resp != "ok" || !called {
		t.Errorf("a public method passes untouched: %v %v", resp, err)
	}
}

func TestInterceptor_RequestIDReachesTheCaller(t *testing.T) {
	ic := authz.NewInterceptor(mapVerifier{claims: map[string]*authz.Claims{"t": func() *authz.Claims {
		c := claims(acme, authz.ScopeMemoryWrite)
		c.AllNamespaces = true
		return c
	}()}}, fixtureResolver(), nil, authz.DefaultPolicy())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bearer t"))
	req := &memoryv1.RetainRequest{Namespace: ref(acme, a1), Meta: &memoryv1.RequestMeta{RequestId: "req-42"}}
	var got authz.RequestScope
	_, err := ic.Unary()(ctx, req, &grpc.UnaryServerInfo{FullMethod: mRetain},
		func(ctx context.Context, _ any) (any, error) { got, _ = authz.FromContext(ctx); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if got.Caller.RequestID != "req-42" || got.Caller.Subject != "u-acme" || len(got.Scopes) != 1 {
		t.Errorf("caller = %+v scopes = %v", got.Caller, got.Scopes)
	}
}
