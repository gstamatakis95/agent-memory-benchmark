package authz

import (
	"context"
	"crypto"
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// JWTOptions configure the production TokenVerifier.
type JWTOptions struct {
	// Issuer is the required `iss`. Audiences are the accepted `aud` values: a tenant listener and the operator route
	// each configure their own, which is what keeps an operator token off the tenant route (4.1.1).
	Issuer    string
	Audiences []string
	Keys      KeySource
	// Leeway tolerates clock skew on exp and nbf (default 30 s).
	Leeway time.Duration
	Clock  func() time.Time
}

// JWTVerifier verifies EdDSA and RS256 tokens against a KeySource: signature, `exp` (required), `nbf`, issuer and
// audience. It never touches the catalog. Every rejection is UNAUTHENTICATED with a message that does not say which
// check failed, except an expired token, which says `token expired` so that clients know to refresh (4.1.1).
type JWTVerifier struct{ o JWTOptions }

var _ TokenVerifier = (*JWTVerifier)(nil)

// NewJWTVerifier builds the verifier.
func NewJWTVerifier(o JWTOptions) *JWTVerifier {
	if o.Leeway == 0 {
		o.Leeway = 30 * time.Second
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &JWTVerifier{o: o}
}

// tokenClaims are the claims of an Engram token (D13, N65, N167).
type tokenClaims struct {
	jwt.RegisteredClaims
	TenantID string   `json:"tenant_id"`
	NS       []string `json:"ns"`
	NSGroup  []string `json:"ns_group"`
	Scopes   []string `json:"scopes"`
	Cell     string   `json:"cell"`
}

// Verify implements TokenVerifier.
func (v *JWTVerifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	if raw == "" {
		return nil, errs.Unauthenticated("missing bearer token")
	}
	var c tokenClaims
	var keyErr error
	_, err := jwt.ParseWithClaims(raw, &c, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errUnknownKey
		}
		var k crypto.PublicKey
		k, keyErr = v.o.Keys.Key(ctx, kid)
		return k, keyErr
	}, jwt.WithValidMethods([]string{"EdDSA", "RS256"}), jwt.WithExpirationRequired(), jwt.WithIssuer(v.o.Issuer),
		jwt.WithLeeway(v.o.Leeway), jwt.WithTimeFunc(v.o.Clock))
	if err != nil {
		var typed *errs.Error
		if errors.As(keyErr, &typed) {
			return nil, keyErr // the key source is unavailable: not a verdict on the token
		}
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, errs.Unauthenticated("token expired")
		}
		return nil, errs.Unauthenticated("invalid token")
	}
	if !v.audienceOK(c.Audience) {
		return nil, errs.Unauthenticated("invalid token")
	}
	claims, err := claimsOf(&c)
	if err != nil {
		return nil, errs.Unauthenticated("invalid token")
	}
	return claims, nil
}

func (v *JWTVerifier) audienceOK(aud jwt.ClaimStrings) bool {
	if len(v.o.Audiences) == 0 {
		return true
	}
	for _, a := range aud {
		for _, want := range v.o.Audiences {
			if a == want {
				return true
			}
		}
	}
	return false
}

// claimsOf converts the wire claims to Claims. A token that mixes the tenant and the fleet worlds is rejected: a tenant
// token (tenant_id) never carries engram.operator or engram.worker, and a fleet token carries no tenant_id (4.1.1,
// N167).
func claimsOf(c *tokenClaims) (*Claims, error) {
	out := &Claims{Subject: c.Subject, Groups: c.NSGroup, Cell: id.CellID(c.Cell)}
	if c.ExpiresAt != nil {
		out.ExpiresAt = c.ExpiresAt.Time
	}
	if c.TenantID != "" {
		t, err := id.ParseTenantID(c.TenantID)
		if err != nil {
			return nil, err
		}
		out.Tenant = t
	}
	for _, s := range c.NS {
		if s == "*" {
			out.AllNamespaces = true
			continue
		}
		ns, err := id.ParseNamespaceID(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}
		out.Namespaces = append(out.Namespaces, ns)
	}
	fleet := false
	for _, s := range c.Scopes {
		sc := Scope(s)
		out.Scopes = append(out.Scopes, sc)
		fleet = fleet || sc == ScopeOperator || sc == ScopeWorker
	}
	if fleet && out.Tenant != "" {
		return nil, errors.New("a tenant token carries a fleet scope")
	}
	return out, nil
}
