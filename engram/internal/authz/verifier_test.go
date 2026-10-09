package authz_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/errs"
)

const (
	issuer   = "https://issuer.example"
	audience = "engram-tenant"
)

type signer struct {
	kid string
	key any // ed25519.PrivateKey or *rsa.PrivateKey
	alg jwt.SigningMethod
}

func newEd(t testing.TB, kid string) signer {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return signer{kid, k, jwt.SigningMethodEdDSA}
}

func newRSA(t testing.TB, kid string) signer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return signer{kid, k, jwt.SigningMethodRS256}
}

func (s signer) jwk() map[string]any {
	switch k := s.key.(type) {
	case ed25519.PrivateKey:
		return map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": s.kid, "use": "sig",
			"x": base64.RawURLEncoding.EncodeToString(k.Public().(ed25519.PublicKey))}
	case *rsa.PrivateKey:
		return map[string]any{"kty": "RSA", "kid": s.kid, "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(k.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.E)).Bytes())}
	}
	panic("key type")
}

func (s signer) token(t testing.TB, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(s.alg, claims)
	tok.Header["kid"] = s.kid
	out, err := tok.SignedString(s.key)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

var now0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func good(extra jwt.MapClaims) jwt.MapClaims {
	c := jwt.MapClaims{"iss": issuer, "aud": audience, "sub": "user-1", "exp": now0.Add(time.Hour).Unix(),
		"iat": now0.Unix(), "tenant_id": "acme", "ns": []string{"*"}, "ns_group": []string{"sales"},
		"scopes": []string{"memory.read", "memory.write"}}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

// jwksServer serves a mutable key set and counts fetches.
type jwksServer struct {
	*httptest.Server
	mu      sync.Mutex
	keys    []signer
	fetches atomic.Int32
	down    atomic.Bool
}

func newJWKS(t testing.TB, keys ...signer) *jwksServer {
	s := &jwksServer{keys: keys}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.fetches.Add(1)
		if s.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		var set []map[string]any
		for _, k := range s.keys {
			set = append(set, k.jwk())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": set})
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *jwksServer) rotate(keys ...signer) {
	s.mu.Lock()
	s.keys = keys
	s.mu.Unlock()
}

func verifierFor(t testing.TB, s *jwksServer, clk *fakeNow) *authz.JWTVerifier {
	cache := authz.NewJWKSCache(authz.JWKSOptions{URL: s.URL, Clock: clk.Now})
	return authz.NewJWTVerifier(authz.JWTOptions{Issuer: issuer, Audiences: []string{audience}, Keys: cache,
		Clock: clk.Now, Leeway: time.Second})
}

type fakeNow struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeNow) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeNow) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

func TestJWT_VerifiesEdDSAAndRS256(t *testing.T) {
	ed, rs := newEd(t, "ed-1"), newRSA(t, "rs-1")
	srv := newJWKS(t, ed, rs)
	v := verifierFor(t, srv, &fakeNow{t: now0})
	for _, s := range []signer{ed, rs} {
		c, err := v.Verify(context.Background(), s.token(t, good(nil)))
		if err != nil {
			t.Fatalf("%s: %v", s.kid, err)
		}
		if c.Tenant != "acme" || !c.AllNamespaces || len(c.Groups) != 1 || c.Groups[0] != "sales" ||
			c.Subject != "user-1" || len(c.Scopes) != 2 || !c.ExpiresAt.Equal(now0.Add(time.Hour)) {
			t.Errorf("%s: claims = %+v", s.kid, c)
		}
	}
}

func TestJWT_NamespaceListAndGroupClaims(t *testing.T) {
	ed := newEd(t, "ed-1")
	v := verifierFor(t, newJWKS(t, ed), &fakeNow{t: now0})
	c, err := v.Verify(context.Background(), ed.token(t, good(jwt.MapClaims{
		"ns": []string{a1.String(), a2.String()}, "ns_group": []string{"eng", "sales"}})))
	if err != nil || c.AllNamespaces || len(c.Namespaces) != 2 || c.Namespaces[0] != a1 || len(c.Groups) != 2 {
		t.Fatalf("claims = %+v, %v", c, err)
	}
	bad := ed.token(t, good(jwt.MapClaims{"ns": []string{"not-a-uuid"}}))
	if _, err := v.Verify(context.Background(), bad); err == nil {
		t.Error("a malformed namespace id in ns must reject the token")
	}
}

func TestJWT_Rejections(t *testing.T) {
	ed, other := newEd(t, "ed-1"), newEd(t, "ed-1") // same kid, different key
	srv := newJWKS(t, ed)
	clk := &fakeNow{t: now0}
	v := verifierFor(t, srv, clk)
	tests := []struct {
		name  string
		token func() string
		want  string
	}{
		{"expired", func() string { return ed.token(t, good(jwt.MapClaims{"exp": now0.Add(-time.Minute).Unix()})) },
			"token expired"},
		{"no exp", func() string { return ed.token(t, good(jwt.MapClaims{"exp": nil})) }, "invalid token"},
		{"not yet valid", func() string { return ed.token(t, good(jwt.MapClaims{"nbf": now0.Add(time.Hour).Unix()})) },
			"invalid token"},
		{"wrong issuer", func() string { return ed.token(t, good(jwt.MapClaims{"iss": "https://evil"})) },
			"invalid token"},
		{"wrong audience", func() string { return ed.token(t, good(jwt.MapClaims{"aud": "engram-operator"})) },
			"invalid token"},
		{"signed by another key", func() string { return other.token(t, good(nil)) }, "invalid token"},
		{"unsigned (alg none)", func() string {
			tok := jwt.NewWithClaims(jwt.SigningMethodNone, good(nil))
			tok.Header["kid"] = "ed-1"
			s, _ := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
			return s
		}, "invalid token"},
		{"HS256 signed with the public key (alg confusion)", func() string {
			tok := jwt.NewWithClaims(jwt.SigningMethodHS256, good(nil))
			tok.Header["kid"] = "ed-1"
			s, _ := tok.SignedString([]byte(ed.key.(ed25519.PrivateKey).Public().(ed25519.PublicKey)))
			return s
		}, "invalid token"},
		{"unknown kid", func() string { s := ed; s.kid = "nope"; return s.token(t, good(nil)) }, "invalid token"},
		{"not a jwt", func() string { return "a.b.c" }, "invalid token"},
		{"tenant token with a fleet scope", func() string {
			return ed.token(t, good(jwt.MapClaims{"scopes": []string{"engram.operator"}}))
		}, "invalid token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), tc.token())
			if !errs.Is(err, errs.KindUnauthenticated) || errs.ToStatus(err).Message() != tc.want {
				t.Errorf("err = %v; want UNAUTHENTICATED %q", err, tc.want)
			}
		})
	}
	if _, err := v.Verify(context.Background(), ""); !errs.Is(err, errs.KindUnauthenticated) {
		t.Errorf("empty token: %v", err)
	}
	// The clock decides expiry: the same token is good until its exp and a leeway later.
	tok := ed.token(t, good(nil))
	clk.Advance(time.Hour)
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Errorf("at exp the leeway still admits the token: %v", err)
	}
	clk.Advance(2 * time.Second)
	if _, err := v.Verify(context.Background(), tok); errs.ToStatus(err).Message() != "token expired" {
		t.Errorf("past exp + leeway: %v", err)
	}
}

func TestJWT_OperatorTokenHasNoTenant(t *testing.T) {
	ed := newEd(t, "ed-1")
	v := authz.NewJWTVerifier(authz.JWTOptions{Issuer: issuer, Audiences: []string{"engram-operator"},
		Keys: authz.StaticKeys{"ed-1": ed.key.(ed25519.PrivateKey).Public()}, Clock: (&fakeNow{t: now0}).Now})
	op := good(jwt.MapClaims{"aud": "engram-operator", "tenant_id": nil, "ns": nil, "ns_group": nil,
		"scopes": []string{"engram.operator"}})
	c, err := v.Verify(context.Background(), ed.token(t, op))
	if err != nil || c.Tenant != "" || len(c.Scopes) != 1 || c.Scopes[0] != authz.ScopeOperator {
		t.Fatalf("operator claims = %+v, %v", c, err)
	}
	// ...and the tenant audience is refused on this verifier: an operator route never accepts a tenant token.
	if _, err := v.Verify(context.Background(), ed.token(t, good(nil))); err == nil {
		t.Error("a tenant-audience token must not verify on the operator audience")
	}
	// A worker token carries its cell.
	w := good(jwt.MapClaims{"aud": "engram-operator", "tenant_id": nil, "ns": nil, "ns_group": nil, "cell": "cell-a",
		"scopes": []string{"engram.worker"}})
	if c, err := v.Verify(context.Background(), ed.token(t, w)); err != nil || c.Cell != "cell-a" {
		t.Fatalf("worker claims = %+v, %v", c, err)
	}
}

func TestJWKS_KidRotationAndRefresh(t *testing.T) {
	old, next := newEd(t, "k1"), newEd(t, "k2")
	srv := newJWKS(t, old)
	clk := &fakeNow{t: now0}
	v := verifierFor(t, srv, clk)
	ctx := context.Background()
	if _, err := v.Verify(ctx, old.token(t, good(nil))); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(ctx, old.token(t, good(nil))); err != nil || srv.fetches.Load() != 1 {
		t.Fatalf("a cached key set is not refetched: %d fetches, %v", srv.fetches.Load(), err)
	}

	// Rotation: the issuer starts signing with k2 and publishes {k1, k2}. The unknown kid forces a refresh...
	srv.rotate(old, next)
	clk.Advance(11 * time.Second) // past MinRefresh
	if _, err := v.Verify(ctx, next.token(t, good(nil))); err != nil {
		t.Fatalf("a token with a new kid must verify after one refresh: %v", err)
	}
	// ...but a flood of unknown kids cannot turn the verifier into a request amplifier.
	before := srv.fetches.Load()
	bogus := old
	bogus.kid = "random"
	for range 50 {
		_, _ = v.Verify(ctx, bogus.token(t, good(nil)))
	}
	if n := srv.fetches.Load() - before; n > 1 {
		t.Errorf("%d fetches for 50 unknown kids inside MinRefresh; want at most 1", n)
	}
	// Retiring k1: after the TTL (5 min) the background refresh drops it.
	srv.rotate(next)
	clk.Advance(6 * time.Minute)
	_, _ = v.Verify(ctx, next.token(t, good(nil))) // triggers the background refresh
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := v.Verify(ctx, old.token(t, good(nil))); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("a retired kid must stop verifying once the refreshed set no longer holds it")
}

func TestJWKS_IssuerOutage(t *testing.T) {
	ed := newEd(t, "k1")
	srv := newJWKS(t, ed)
	clk := &fakeNow{t: now0}
	v := verifierFor(t, srv, clk)
	ctx := context.Background()
	if _, err := v.Verify(ctx, ed.token(t, good(nil))); err != nil {
		t.Fatal(err)
	}
	srv.down.Store(true)
	clk.Advance(30 * time.Minute) // far past the TTL: the old set keeps serving while the refresh keeps failing
	for range 3 {
		tok := ed.token(t, good(jwt.MapClaims{"exp": now0.Add(2 * time.Hour).Unix()}))
		if _, err := v.Verify(ctx, tok); err != nil {
			t.Fatalf("an issuer outage must not log everybody out: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// An issuer that is down sees at most one request per MinRefresh, not one per verified token.
	before := srv.fetches.Load()
	for range 100 {
		_, _ = v.Verify(ctx, ed.token(t, good(jwt.MapClaims{"exp": now0.Add(2 * time.Hour).Unix()})))
	}
	time.Sleep(50 * time.Millisecond)
	if n := srv.fetches.Load() - before; n > 2 {
		t.Errorf("%d JWKS fetches for 100 verifications during an outage; want at most 2", n)
	}

	// With no key set at all, the verifier cannot decide: UNAVAILABLE, not UNAUTHENTICATED.
	cold := verifierFor(t, srv, &fakeNow{t: now0})
	_, err := cold.Verify(ctx, ed.token(t, good(nil)))
	if !errs.Is(err, errs.KindUnavailable) {
		t.Errorf("cold verifier with the issuer down = %v; want UNAVAILABLE", err)
	}
}

func TestParseJWKS(t *testing.T) {
	ed := newEd(t, "k1")
	rs := newRSA(t, "k2")
	short := map[string]any{"kty": "RSA", "kid": "weak", "n": base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}),
		"e": "AQAB"}
	enc := ed.jwk()
	enc["kid"], enc["use"] = "enc-key", "enc"
	doc, _ := json.Marshal(map[string]any{"keys": []any{ed.jwk(), rs.jwk(), short, enc,
		map[string]any{"kty": "EC", "kid": "ec", "crv": "P-256"},
		map[string]any{"kty": "OKP", "crv": "Ed25519", "x": "AA"}}})
	keys, err := authz.ParseJWKS(doc)
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys = %v, %v; want exactly the Ed25519 and the 2048-bit RSA key", keys, err)
	}
	if _, err := authz.ParseJWKS([]byte(`{"keys":[]}`)); err == nil {
		t.Error("an empty set is an error")
	}
	if _, err := authz.ParseJWKS([]byte(`nope`)); err == nil {
		t.Error("garbage is an error")
	}
}
