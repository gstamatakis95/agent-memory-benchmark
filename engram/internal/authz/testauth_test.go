//go:build testauth

package authz_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gstamatakis95/engram/internal/authz"
)

// TestAllowAllVerifier: with the testauth tag, claims come from the x-test-claims header and nothing else is verified;
// the interceptor still applies every policy rule to them.
func TestAllowAllVerifier(t *testing.T) {
	ic := authz.NewInterceptor(authz.AllowAllVerifier{}, fixtureResolver(), nil, authz.DefaultPolicy())
	call := func(claims string) (authz.RequestScope, error) {
		md := metadata.Pairs()
		if claims != "" {
			md.Set("x-test-claims", claims)
		}
		var rs authz.RequestScope
		_, err := ic.Unary()(metadata.NewIncomingContext(context.Background(), md), getMemory(acme, a1),
			&grpc.UnaryServerInfo{FullMethod: mGetMemory},
			func(ctx context.Context, _ any) (any, error) { rs, _ = authz.FromContext(ctx); return nil, nil })
		return rs, err
	}
	rs, err := call(`{"sub":"t","tenant_id":"acme","ns":["*"],"scopes":["memory.read"]}`)
	if err != nil || rs.Tenant != acme || rs.Namespace != a1 {
		t.Fatalf("scope = %+v, %v", rs, err)
	}
	if _, err := call(`{"tenant_id":"acme","ns":["` + a2.String() + `"],"scopes":["memory.read"]}`); status.Code(err) !=
		codes.PermissionDenied {
		t.Errorf("allowlist applies to test claims: %v", err)
	}
	_, err = call(`{"tenant_id":"globex","ns":["*"],"scopes":["memory.read"]}`)
	if status.Code(err) != codes.NotFound {
		t.Errorf("tenant isolation applies to test claims: %v", err)
	}
	if _, err := call(""); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no header: %v", err)
	}
	if _, err := call("{not json"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("bad header: %v", err)
	}
}
