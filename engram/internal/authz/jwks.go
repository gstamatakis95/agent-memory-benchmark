package authz

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/gstamatakis95/engram/internal/errs"
)

// KeySource resolves the public key of a token's `kid`.
type KeySource interface {
	Key(ctx context.Context, kid string) (crypto.PublicKey, error)
}

// StaticKeys is a fixed KeySource (tests, and deployments that pin their keys in configuration).
type StaticKeys map[string]crypto.PublicKey

// Key implements KeySource.
func (s StaticKeys) Key(_ context.Context, kid string) (crypto.PublicKey, error) {
	if k, ok := s[kid]; ok {
		return k, nil
	}
	return nil, errUnknownKey
}

var errUnknownKey = fmt.Errorf("authz: unknown key id")

// JWKS defaults (D13): the key set is cached for 5 minutes; an unknown kid (a rotation) forces one refresh, at most
// once per MinRefresh, so that a flood of tokens with random kids cannot turn the verifier into a request amplifier.
const (
	DefaultJWKSTTL        = 5 * time.Minute
	DefaultJWKSMinRefresh = 10 * time.Second
	DefaultJWKSTimeout    = 5 * time.Second
)

// JWKSCache is a KeySource over a JWKS endpoint (RFC 7517), supporting OKP/Ed25519 and RSA keys.
//
//   - A cached set is served until its TTL ends; after that it is refreshed in the background and the old set keeps
//     serving until the refresh succeeds (an issuer outage must not log every caller out).
//   - An unknown kid refreshes synchronously, rate-limited by MinRefresh (kid rotation).
//   - With no set at all and the endpoint unreachable, the answer is UNAVAILABLE, not UNAUTHENTICATED: the verifier
//     cannot tell whether the token is good.
type JWKSCache struct {
	url        string
	client     *http.Client
	ttl        time.Duration
	minRefresh time.Duration
	clock      func() time.Time

	fetchMu sync.Mutex // one fetch at a time; callers that arrive meanwhile wait for its outcome

	mu         sync.RWMutex
	keys       map[string]crypto.PublicKey
	fetchedAt  time.Time
	attemptAt  time.Time
	lastErr    error // outcome of the last attempt, returned to callers the MinRefresh limit turns away
	refreshing bool
}

// JWKSOptions configure NewJWKSCache; zero fields take the defaults above.
type JWKSOptions struct {
	URL        string
	Client     *http.Client
	TTL        time.Duration
	MinRefresh time.Duration
	Clock      func() time.Time
}

// NewJWKSCache returns a cache that fetches lazily on first use.
func NewJWKSCache(o JWKSOptions) *JWKSCache {
	if o.Client == nil {
		o.Client = &http.Client{Timeout: DefaultJWKSTimeout}
	}
	if o.TTL <= 0 {
		o.TTL = DefaultJWKSTTL
	}
	if o.MinRefresh <= 0 {
		o.MinRefresh = DefaultJWKSMinRefresh
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &JWKSCache{url: o.URL, client: o.Client, ttl: o.TTL, minRefresh: o.MinRefresh, clock: o.Clock}
}

// Key implements KeySource.
func (c *JWKSCache) Key(ctx context.Context, kid string) (crypto.PublicKey, error) {
	now := c.clock()
	c.mu.RLock()
	k, ok := c.keys[kid]
	have, age := c.keys != nil, now.Sub(c.fetchedAt)
	c.mu.RUnlock()

	if ok {
		if age >= c.ttl {
			c.refreshInBackground()
		}
		return k, nil
	}
	// Unknown kid, or no set yet: refresh synchronously unless a refresh was attempted a moment ago.
	if err := c.refresh(ctx, false); err != nil && !have {
		u := errs.Unavailable("key set unavailable", 2*time.Second)
		u.Cause = err
		return nil, u
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if k, ok := c.keys[kid]; ok {
		return k, nil
	}
	return nil, errUnknownKey
}

func (c *JWKSCache) refreshInBackground() {
	c.mu.Lock()
	// One refresh at a time, and not more often than MinRefresh: an issuer that is down must see one request per
	// interval, not one per verified token.
	if c.refreshing || c.clock().Sub(c.attemptAt) < c.minRefresh {
		c.mu.Unlock()
		return
	}
	c.refreshing = true
	c.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultJWKSTimeout)
		defer cancel()
		_ = c.refresh(ctx, true)
		c.mu.Lock()
		c.refreshing = false
		c.mu.Unlock()
	}()
}

// refresh fetches the set. Unless force is set it does nothing when an attempt was made within MinRefresh.
func (c *JWKSCache) refresh(ctx context.Context, force bool) (err error) {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	c.mu.Lock()
	now := c.clock()
	if !force && !c.attemptAt.IsZero() && now.Sub(c.attemptAt) < c.minRefresh {
		err := c.lastErr // also before the first success: an issuer that is down sees one request per MinRefresh
		c.mu.Unlock()
		return err
	}
	c.attemptAt = now
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only body
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authz: jwks %s: status %d", c.url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	keys, err := ParseJWKS(body)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.keys, c.fetchedAt = keys, c.clock()
	c.mu.Unlock()
	return nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// ParseJWKS decodes a JWK set into the keys the verifier can use (Ed25519 and RSA signing keys with a kid). Keys of
// another type or use are skipped; a set without a single usable key is an error.
func ParseJWKS(doc []byte) (map[string]crypto.PublicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(doc, &set); err != nil {
		return nil, fmt.Errorf("authz: jwks: %w", err)
	}
	out := map[string]crypto.PublicKey{}
	for _, k := range set.Keys {
		if k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		switch {
		case k.Kty == "OKP" && k.Crv == "Ed25519":
			x, err := base64.RawURLEncoding.DecodeString(k.X)
			if err != nil || len(x) != ed25519.PublicKeySize {
				continue
			}
			out[k.Kid] = ed25519.PublicKey(x)
		case k.Kty == "RSA":
			n, err1 := base64.RawURLEncoding.DecodeString(k.N)
			e, err2 := base64.RawURLEncoding.DecodeString(k.E)
			if err1 != nil || err2 != nil || len(n) < 256 { // 2048 bits at least
				continue
			}
			out[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("authz: jwks without a usable signing key")
	}
	return out, nil
}
