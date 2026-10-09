package codec

import (
	"context"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// WrappedKeyStore persists the wrapped per-namespace data keys. Its Shred is irreversible: a shredded namespace gets a
// tombstone, so a later DataKey call can never mint a fresh key for it.
type WrappedKeyStore interface {
	// Get returns the wrapped key, fs.ErrNotExist when there is none.
	Get(ctx context.Context, ns id.NamespaceID, keyID string) ([]byte, error)
	// PutIfAbsent stores wrapped unless a key exists and returns the stored value (the winner of a race).
	PutIfAbsent(ctx context.Context, ns id.NamespaceID, keyID string, wrapped []byte) ([]byte, error)
	// Shred deletes every wrapped key of ns and leaves the tombstone.
	Shred(ctx context.Context, ns id.NamespaceID) error
	// Shredded reports whether ns carries the tombstone.
	Shredded(ctx context.Context, ns id.NamespaceID) (bool, error)
}

// ShardResolver maps a namespace to the shard that hosts it, which selects the codec key that wraps the namespace's
// data key (N59: a per-shard codec key; the section 2.2.17 signatures speak of namespaces only, CONFLICTS.md #29). The
// composition root injects it: StaticShard for the one-shard dev stack, the catalog resolver from M1.x. When a
// namespace moves, new payloads use the target shard's key and carry its kid; old payloads still decode through the
// source shard's key, which must therefore stay available until those histories expire (retention plus the largest
// workflow run timeout, N99); M1.5's move re-wraps the data key under the target shard's key.
type ShardResolver interface {
	ShardOf(ns id.NamespaceID) (id.ShardID, bool)
}

// StaticShard places every namespace on one shard.
type StaticShard id.ShardID

// ShardOf implements ShardResolver.
func (s StaticShard) ShardOf(id.NamespaceID) (id.ShardID, bool) { return id.ShardID(s), true }

// KeyPrefix is the prefix of the kids of a shard's codec keys ("s7-"); the cell key, which encrypts the cell-wide
// workflows that belong to no namespace, uses "cell-".
func KeyPrefix(s id.ShardID) string { return fmt.Sprintf("s%d-", int32(s)) }

const cellPrefix = "cell-"

// KeyCacheTTL is how long a process keeps an unwrapped data key; secrets are re-read every 60 s (section 9.1), and a
// Shred through the same provider drops its own cache at once.
const KeyCacheTTL = 60 * time.Second

// FileKeyProvider is the KeyProvider of the dev stack and of a worker whose codec keys are files under
// /run/secrets/temporal_codec: `<kid>.key` (32 raw bytes or 64 hex characters) per codec key, with kids
// "s<shard>-<ver>" per shard and "cell-<ver>" for the cell-wide workflows, and an optional `s<shard>.current` /
// `cell.current` file naming the kid new payloads use (otherwise the greatest kid with the prefix). A worker only
// holds the keys of the shards it serves, so it cannot decode another shard's payloads. Data keys are random, created
// on first use for the current kid, and stored AES-GCM wrapped under that key by the WrappedKeyStore.
type FileKeyProvider struct {
	dir    string
	store  WrappedKeyStore
	shards ShardResolver
	now    func() time.Time

	mu    sync.Mutex
	cache map[cacheKey]cacheVal
	cur   map[string]cacheCur
}

type cacheCur struct {
	kid string
	at  time.Time
}

type cacheKey struct {
	ns    id.NamespaceID
	keyID string
}

type cacheVal struct {
	key []byte
	at  time.Time
}

// NewFileKeyProvider reads codec keys from dir and keeps wrapped data keys in store.
func NewFileKeyProvider(dir string, store WrappedKeyStore, shards ShardResolver) *FileKeyProvider {
	return &FileKeyProvider{dir: dir, store: store, shards: shards, now: time.Now, cache: map[cacheKey]cacheVal{},
		cur: map[string]cacheCur{}}
}

// CurrentKeyID implements KeyProvider: the cell key for the zero namespace, otherwise the current key of the shard that
// hosts ns. With no readable key, or a namespace the resolver does not know, it returns "", and DataKey then fails
// ErrUnknownKey.
func (p *FileKeyProvider) CurrentKeyID(ns id.NamespaceID) string {
	prefix, name := cellPrefix, "cell"
	if !ns.IsZero() {
		shard, ok := p.shards.ShardOf(ns)
		if !ok {
			return ""
		}
		prefix, name = KeyPrefix(shard), fmt.Sprintf("s%d", int32(shard))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.cur[prefix]; ok && p.now().Sub(c.at) < KeyCacheTTL {
		return c.kid
	}
	kid := ""
	if b, err := os.ReadFile(filepath.Join(p.dir, name+".current")); err == nil {
		kid = strings.TrimSpace(string(b))
	} else if ents, err := os.ReadDir(p.dir); err == nil {
		var kids []string
		for _, e := range ents {
			if k, ok := strings.CutSuffix(e.Name(), ".key"); ok && !e.IsDir() && strings.HasPrefix(k, prefix) {
				kids = append(kids, k)
			}
		}
		sort.Strings(kids)
		if len(kids) > 0 {
			kid = kids[len(kids)-1]
		}
	}
	p.cur[prefix] = cacheCur{kid, p.now()}
	return kid
}

func (p *FileKeyProvider) shardKey(keyID string) ([]byte, error) {
	if keyID == "" || strings.ContainsAny(keyID, "/\\") || strings.HasPrefix(keyID, ".") {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKey, keyID)
	}
	b, err := os.ReadFile(filepath.Join(p.dir, keyID+".key"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKey, keyID)
	}
	if err != nil {
		return nil, err
	}
	if t := strings.TrimSpace(string(b)); len(t) == 64 {
		if raw, err := hex.DecodeString(t); err == nil {
			return raw, nil
		}
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("codec: shard key %q is %d bytes, want 32 raw or 64 hex", keyID, len(b))
	}
	return b, nil
}

// DataKey implements KeyProvider.
func (p *FileKeyProvider) DataKey(ctx context.Context, ns id.NamespaceID, keyID string) ([]byte, error) {
	ck := cacheKey{ns, keyID}
	p.mu.Lock()
	if v, ok := p.cache[ck]; ok && p.now().Sub(v.at) < KeyCacheTTL {
		p.mu.Unlock()
		return v.key, nil
	}
	p.mu.Unlock()
	key, err := p.load(ctx, ns, keyID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.cache[ck] = cacheVal{key, p.now()}
	p.mu.Unlock()
	return key, nil
}

func (p *FileKeyProvider) load(ctx context.Context, ns id.NamespaceID, keyID string) ([]byte, error) {
	shard, err := p.shardKey(keyID)
	if err != nil {
		return nil, err
	}
	if ns.IsZero() {
		// the shard-level key: derived, never stored, never shredded
		return hkdf.Key(sha256.New, shard, nil, "engram/temporal/shard-payload/v1", 32)
	}
	if gone, err := p.store.Shredded(ctx, ns); err != nil {
		return nil, err
	} else if gone {
		return nil, fmt.Errorf("%w: %s", ErrKeyShredded, ns)
	}
	wrapped, err := p.store.Get(ctx, ns, keyID)
	if errors.Is(err, fs.ErrNotExist) {
		if keyID != p.CurrentKeyID(ns) { // a missing key is minted for the current kid only
			return nil, fmt.Errorf("%w: no data key of %s under %q", ErrUnknownKey, ns, keyID)
		}
		fresh := make([]byte, 32)
		if _, err := rand.Read(fresh); err != nil {
			return nil, err
		}
		w, err := wrap(shard, ns, keyID, fresh)
		if err != nil {
			return nil, err
		}
		if wrapped, err = p.store.PutIfAbsent(ctx, ns, keyID, w); err != nil {
			return nil, err // ErrKeyShredded when a Shred overtook this first use
		}
	} else if err != nil {
		return nil, err
	}
	return unwrap(shard, ns, keyID, wrapped)
}

// Shred implements KeyProvider.
func (p *FileKeyProvider) Shred(ctx context.Context, ns id.NamespaceID) error {
	if ns.IsZero() {
		return errors.New("codec: the shard-level key is not shredded per namespace")
	}
	if err := p.store.Shred(ctx, ns); err != nil {
		return err
	}
	p.mu.Lock()
	for k := range p.cache {
		if k.ns == ns {
			delete(p.cache, k)
		}
	}
	p.mu.Unlock()
	return nil
}

func wrapAD(ns id.NamespaceID, keyID string) []byte {
	return []byte("engram/wrap/v1\x00" + nsString(ns) + "\x00" + keyID)
}

func wrap(shard []byte, ns id.NamespaceID, keyID string, dataKey []byte) ([]byte, error) {
	aead, err := newGCM(shard)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, dataKey, wrapAD(ns, keyID)), nil
}

func unwrap(shard []byte, ns id.NamespaceID, keyID string, wrapped []byte) ([]byte, error) {
	aead, err := newGCM(shard)
	if err != nil {
		return nil, err
	}
	if len(wrapped) < aead.NonceSize() {
		return nil, errors.New("codec: wrapped key is too short")
	}
	key, err := aead.Open(nil, wrapped[:aead.NonceSize()], wrapped[aead.NonceSize():], wrapAD(ns, keyID))
	if err != nil {
		return nil, fmt.Errorf("codec: wrapped key of %s does not authenticate under %q: %w", ns, keyID, err)
	}
	return key, nil
}

// MemStore is an in-memory WrappedKeyStore for tests.
type MemStore struct {
	mu   sync.Mutex
	keys map[cacheKey][]byte
	gone map[id.NamespaceID]bool
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{keys: map[cacheKey][]byte{}, gone: map[id.NamespaceID]bool{}}
}

// Get implements WrappedKeyStore.
func (m *MemStore) Get(_ context.Context, ns id.NamespaceID, keyID string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.keys[cacheKey{ns, keyID}]; ok {
		return b, nil
	}
	return nil, fs.ErrNotExist
}

// PutIfAbsent implements WrappedKeyStore.
func (m *MemStore) PutIfAbsent(_ context.Context, ns id.NamespaceID, keyID string, w []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gone[ns] {
		return nil, fmt.Errorf("%w: %s", ErrKeyShredded, ns)
	}
	if b, ok := m.keys[cacheKey{ns, keyID}]; ok {
		return b, nil
	}
	m.keys[cacheKey{ns, keyID}] = w
	return w, nil
}

// Shred implements WrappedKeyStore.
func (m *MemStore) Shred(_ context.Context, ns id.NamespaceID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.keys {
		if k.ns == ns {
			delete(m.keys, k)
		}
	}
	m.gone[ns] = true
	return nil
}

// Shredded implements WrappedKeyStore.
func (m *MemStore) Shredded(_ context.Context, ns id.NamespaceID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gone[ns], nil
}

// DirStore keeps wrapped keys as files `<dir>/<ns>/<kid>.wrapped` and the tombstone as `<dir>/<ns>/SHREDDED`.
type DirStore struct{ dir string }

// NewDirStore returns a store rooted at dir, which must be writable by the process.
func NewDirStore(dir string) *DirStore { return &DirStore{dir: dir} }

func (d *DirStore) nsDir(ns id.NamespaceID) string { return filepath.Join(d.dir, ns.String()) }

// Get implements WrappedKeyStore.
func (d *DirStore) Get(_ context.Context, ns id.NamespaceID, keyID string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.nsDir(ns), keyID+".wrapped"))
}

// PutIfAbsent implements WrappedKeyStore with an exclusive create, so concurrent workers agree on one key.
func (d *DirStore) PutIfAbsent(ctx context.Context, ns id.NamespaceID, keyID string, w []byte) ([]byte, error) {
	if err := os.MkdirAll(d.nsDir(ns), 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(d.nsDir(ns), keyID+".wrapped")
	tmp, err := os.CreateTemp(d.nsDir(ns), ".tmp-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(w); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	// link(2) fails when the target exists: the first writer wins and the others read its value
	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return d.Get(ctx, ns, keyID)
		}
		return nil, err
	}
	// Shred writes the tombstone before it deletes the wrapped files, so a key linked after Shred listed the directory
	// is caught by this re-check, and one linked before is deleted by Shred: no wrapped key outlives a shred.
	if gone, err := d.Shredded(ctx, ns); err != nil || gone {
		_ = os.Remove(path)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrKeyShredded, ns)
	}
	return w, nil
}

// Shred implements WrappedKeyStore.
func (d *DirStore) Shred(_ context.Context, ns id.NamespaceID) error {
	if err := os.MkdirAll(d.nsDir(ns), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(d.nsDir(ns), "SHREDDED"), nil, 0o600); err != nil {
		return err
	}
	ents, err := os.ReadDir(d.nsDir(ns))
	if err != nil {
		return err
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".wrapped") {
			if err := os.Remove(filepath.Join(d.nsDir(ns), e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Shredded implements WrappedKeyStore.
func (d *DirStore) Shredded(_ context.Context, ns id.NamespaceID) (bool, error) {
	_, err := os.Stat(filepath.Join(d.nsDir(ns), "SHREDDED"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// Store returns the WrappedKeyStore, so a second provider (another process in a test, a rotation) can share it.
func (p *FileKeyProvider) Store() WrappedKeyStore { return p.store }
