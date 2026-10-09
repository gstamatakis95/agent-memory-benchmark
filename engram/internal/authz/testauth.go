//go:build testauth

package authz

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gstamatakis95/engram/internal/errs"
)

// TestClaimsHeader is the header AllowAllVerifier reads its claims from.
const TestClaimsHeader = "x-test-claims"

// AllowAllVerifier is the test TokenVerifier of PLAN.md section 8.1: it accepts any call and takes the claims from the
// JSON in the x-test-claims header ({"sub", "tenant_id", "ns", "ns_group", "scopes", "cell"}). It does not look at the
// bearer token and verifies nothing, which is why it exists only behind the testauth build tag and is never compiled
// into a release image.
type AllowAllVerifier struct{}

var _ TokenVerifier = AllowAllVerifier{}

// Verify implements TokenVerifier.
func (AllowAllVerifier) Verify(ctx context.Context, _ string) (*Claims, error) {
	get := HeaderFromContext(ctx)
	raw := ""
	if get != nil {
		raw = get(TestClaimsHeader)
	}
	if raw == "" {
		return nil, errs.Unauthenticated("missing " + TestClaimsHeader)
	}
	var c tokenClaims
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, errs.Unauthenticated("bad " + TestClaimsHeader)
	}
	claims, err := claimsOf(&c)
	if err != nil {
		return nil, errs.Unauthenticated("bad " + TestClaimsHeader)
	}
	claims.ExpiresAt = time.Now().Add(time.Hour)
	return claims, nil
}
