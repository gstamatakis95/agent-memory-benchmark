package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNoSnapshot is returned by Runtime.Search when nothing is loaded yet.
var ErrNoSnapshot = errors.New("snapshot: no snapshot loaded")

// runtimeState is the mutable, lock-guarded part of a Runtime. openFn and
// fetchFn default to Open and the store's fetch respectively; tests
// substitute fakes so Runtime's swap/lock semantics can be exercised
// without a real artifact.
type runtimeState struct {
	mu       sync.RWMutex
	snap     Snapshot
	manifest Manifest

	openFn  func(dir string) (Snapshot, error)
	fetchFn func(ctx context.Context, p Pointer, cacheDir string) (string, error)
}

func newRuntime(store *S3Store, cacheDir string, expect Expectations) *Runtime {
	r := &Runtime{store: store, cacheDir: cacheDir, Expect: expect}
	r.rt.openFn = Open
	r.rt.fetchFn = func(ctx context.Context, p Pointer, cacheDir string) (string, error) {
		return store.fetch(ctx, p, cacheDir)
	}
	return r
}

// checkExpectations refuses a load when the snapshot was not built with the
// pinned vector width / embed model (docs/07 section 7: a silently
// different embedding space degrades the dense arm to noise with no error
// anywhere).
func (r *Runtime) checkExpectations(m Manifest) error {
	if r.Expect.VectorDim != 0 && m.VectorDim != r.Expect.VectorDim {
		return fmt.Errorf("snapshot: vector dim mismatch: snapshot has %d, runtime expects %d",
			m.VectorDim, r.Expect.VectorDim)
	}
	if r.Expect.EmbedModel != "" && m.EmbedModel != r.Expect.EmbedModel {
		return fmt.Errorf("snapshot: embed model mismatch: snapshot has %q, runtime expects %q",
			m.EmbedModel, r.Expect.EmbedModel)
	}
	return nil
}

// load fetches and opens the given published version and swaps it in. The
// fetch/open/expectation-check happen OUTSIDE the write lock (they can be
// slow — a download, a Bleve open); only the pointer swap itself is
// serialized against concurrent Search calls.
func (r *Runtime) load(ctx context.Context, p Pointer) (Manifest, error) {
	dir, err := r.rt.fetchFn(ctx, p, r.cacheDir)
	if err != nil {
		return Manifest{}, fmt.Errorf("snapshot: fetch version %d: %w", p.Version, err)
	}
	snap, err := r.rt.openFn(dir)
	if err != nil {
		return Manifest{}, fmt.Errorf("snapshot: open version %d: %w", p.Version, err)
	}
	m := snap.Manifest()
	if err := r.checkExpectations(m); err != nil {
		_ = snap.Close()
		return Manifest{}, err
	}

	r.rt.mu.Lock()
	prev := r.rt.snap
	r.rt.snap = snap
	r.rt.manifest = m
	r.rt.mu.Unlock()

	if prev != nil {
		_ = prev.Close() // best-effort; the new snapshot is already live
	}
	return m, nil
}

// reload reads the current pointer and loads that version if it differs
// from the loaded one.
func (r *Runtime) reload(ctx context.Context) (Manifest, bool, error) {
	p, ok, err := r.store.current(ctx)
	if err != nil {
		return Manifest{}, false, fmt.Errorf("snapshot: read current pointer: %w", err)
	}
	if !ok {
		return Manifest{}, false, nil
	}
	if cur, loaded := r.currentManifest(); loaded && cur.Version == p.Version {
		return cur, true, nil
	}
	m, err := r.load(ctx, p)
	if err != nil {
		return Manifest{}, false, err
	}
	return m, true, nil
}

// currentManifest returns the loaded manifest; ok is false when nothing is
// loaded.
func (r *Runtime) currentManifest() (Manifest, bool) {
	r.rt.mu.RLock()
	defer r.rt.mu.RUnlock()
	if r.rt.snap == nil {
		return Manifest{}, false
	}
	return r.rt.manifest, true
}

// search runs fn against the loaded snapshot under the read lock, so a
// concurrent swap (Load) cannot close the snapshot mid-query.
func (r *Runtime) search(ctx context.Context, fn func(Snapshot) error) error {
	r.rt.mu.RLock()
	defer r.rt.mu.RUnlock()
	if r.rt.snap == nil {
		return ErrNoSnapshot
	}
	return fn(r.rt.snap)
}

// close closes the loaded snapshot, if any.
func (r *Runtime) close() error {
	r.rt.mu.Lock()
	snap := r.rt.snap
	r.rt.snap = nil
	r.rt.manifest = Manifest{}
	r.rt.mu.Unlock()
	if snap == nil {
		return nil
	}
	return snap.Close()
}
