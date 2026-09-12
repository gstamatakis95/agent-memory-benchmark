package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSnapshot is a minimal Snapshot implementation for exercising Runtime's
// swap/lock semantics without a real artifact (the artifact side of this
// package — builder.go/open.go/seal.go/search.go — is owned by another
// agent and must not be depended on here).
type fakeSnapshot struct {
	manifest Manifest
	closed   atomic.Bool
	onSearch func()
}

func (f *fakeSnapshot) Manifest() Manifest                   { return f.manifest }
func (f *fakeSnapshot) Len() int                             { return 0 }
func (f *fakeSnapshot) Doc(ordinal int) Doc                  { return Doc{} }
func (f *fakeSnapshot) OrdinalOf(memoryID int64) (int, bool) { return 0, false }
func (f *fakeSnapshot) ConversationFilter(conversationID string) *Filter {
	return nil
}
func (f *fakeSnapshot) LexicalTopN(ctx context.Context, lexemes []string, n int, filt *Filter) ([]Hit, error) {
	return nil, nil
}
func (f *fakeSnapshot) VectorTopN(ctx context.Context, q []float32, n int, filt *Filter) ([]Hit, error) {
	return nil, nil
}
func (f *fakeSnapshot) Search(ctx context.Context, lexemes []string, qvec []float32, filt *Filter, opts SearchOptions) (Result, error) {
	if f.onSearch != nil {
		f.onSearch()
	}
	return Result{}, nil
}
func (f *fakeSnapshot) Close() error {
	f.closed.Store(true)
	return nil
}

// fakeTransport wires a Runtime's fetchFn/openFn to a small in-memory table
// of version -> Snapshot, so load/reload can be exercised deterministically.
func fakeTransport(snaps map[int64]Snapshot) (
	fetchFn func(ctx context.Context, p Pointer, cacheDir string) (string, error),
	openFn func(dir string) (Snapshot, error),
) {
	fetchFn = func(ctx context.Context, p Pointer, cacheDir string) (string, error) {
		if _, ok := snaps[p.Version]; !ok {
			return "", fmt.Errorf("fake transport: no snapshot registered for version %d", p.Version)
		}
		return fmt.Sprintf("dir-v%d", p.Version), nil
	}
	openFn = func(dir string) (Snapshot, error) {
		var v int64
		if _, err := fmt.Sscanf(dir, "dir-v%d", &v); err != nil {
			return nil, err
		}
		return snaps[v], nil
	}
	return fetchFn, openFn
}

func TestRuntimeSearchNoSnapshotYieldsErrNoSnapshot(t *testing.T) {
	r := NewRuntime(nil, t.TempDir(), Expectations{})
	err := r.Search(context.Background(), func(Snapshot) error {
		t.Fatal("fn should not run when nothing is loaded")
		return nil
	})
	if !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Search error = %v, want ErrNoSnapshot", err)
	}
}

func TestRuntimeLoadSwapsAndClosesPrevious(t *testing.T) {
	first := &fakeSnapshot{manifest: Manifest{Version: 1, VectorDim: 8, EmbedModel: "m"}}
	second := &fakeSnapshot{manifest: Manifest{Version: 2, VectorDim: 8, EmbedModel: "m"}}
	fetchFn, openFn := fakeTransport(map[int64]Snapshot{1: first, 2: second})

	r := NewRuntime(nil, t.TempDir(), Expectations{})
	r.rt.fetchFn, r.rt.openFn = fetchFn, openFn

	m1, err := r.Load(context.Background(), Pointer{Version: 1, Key: "k1"})
	if err != nil {
		t.Fatalf("Load v1: %v", err)
	}
	if m1.Version != 1 {
		t.Fatalf("m1.Version = %d, want 1", m1.Version)
	}
	if cur, ok := r.Current(); !ok || cur.Version != 1 {
		t.Fatalf("Current() = %+v, %v; want version 1, true", cur, ok)
	}

	m2, err := r.Load(context.Background(), Pointer{Version: 2, Key: "k2"})
	if err != nil {
		t.Fatalf("Load v2: %v", err)
	}
	if m2.Version != 2 {
		t.Fatalf("m2.Version = %d, want 2", m2.Version)
	}
	if !first.closed.Load() {
		t.Fatal("expected the superseded (v1) snapshot to be closed after the swap")
	}
	if second.closed.Load() {
		t.Fatal("the newly loaded (v2) snapshot must stay open")
	}
	if cur, ok := r.Current(); !ok || cur.Version != 2 {
		t.Fatalf("Current() = %+v, %v; want version 2, true", cur, ok)
	}
}

func TestRuntimeLoadRefusesExpectationMismatch(t *testing.T) {
	bad := &fakeSnapshot{manifest: Manifest{Version: 1, VectorDim: 256, EmbedModel: "wrong-model"}}
	fetchFn, openFn := fakeTransport(map[int64]Snapshot{1: bad})

	r := NewRuntime(nil, t.TempDir(), Expectations{VectorDim: 768, EmbedModel: "nomic-embed-text-v1.5"})
	r.rt.fetchFn, r.rt.openFn = fetchFn, openFn

	_, err := r.Load(context.Background(), Pointer{Version: 1, Key: "k1"})
	if err == nil {
		t.Fatal("expected an Expectations mismatch error")
	}
	if !strings.Contains(err.Error(), "256") || !strings.Contains(err.Error(), "768") {
		t.Fatalf("error should name both dims: %v", err)
	}
	if !bad.closed.Load() {
		t.Fatal("a rejected snapshot must still be closed")
	}
	if _, ok := r.Current(); ok {
		t.Fatal("nothing should be loaded after a rejected Load")
	}
}

func TestRuntimeLoadRefusesEmbedModelMismatch(t *testing.T) {
	bad := &fakeSnapshot{manifest: Manifest{Version: 1, VectorDim: 768, EmbedModel: "other-model"}}
	fetchFn, openFn := fakeTransport(map[int64]Snapshot{1: bad})

	r := NewRuntime(nil, t.TempDir(), Expectations{EmbedModel: "nomic-embed-text-v1.5"})
	r.rt.fetchFn, r.rt.openFn = fetchFn, openFn

	_, err := r.Load(context.Background(), Pointer{Version: 1, Key: "k1"})
	if err == nil {
		t.Fatal("expected an Expectations mismatch error")
	}
	if !strings.Contains(err.Error(), "other-model") || !strings.Contains(err.Error(), "nomic-embed-text-v1.5") {
		t.Fatalf("error should name both models: %v", err)
	}
	if !bad.closed.Load() {
		t.Fatal("a rejected snapshot must still be closed")
	}
}

// TestRuntimeSearchBlocksConcurrentLoad verifies Search holds the read lock
// for its duration, so a concurrent Load cannot complete its swap (and thus
// cannot close the in-use snapshot) until Search returns.
func TestRuntimeSearchBlocksConcurrentLoad(t *testing.T) {
	first := &fakeSnapshot{manifest: Manifest{Version: 1}}
	second := &fakeSnapshot{manifest: Manifest{Version: 2}}
	fetchFn, openFn := fakeTransport(map[int64]Snapshot{1: first, 2: second})

	r := NewRuntime(nil, t.TempDir(), Expectations{})
	r.rt.fetchFn, r.rt.openFn = fetchFn, openFn

	if _, err := r.Load(context.Background(), Pointer{Version: 1, Key: "k1"}); err != nil {
		t.Fatalf("Load v1: %v", err)
	}

	searchStarted := make(chan struct{})
	releaseSearch := make(chan struct{})
	first.onSearch = func() {
		close(searchStarted)
		<-releaseSearch
	}

	searchDone := make(chan struct{})
	go func() {
		defer close(searchDone)
		err := r.Search(context.Background(), func(s Snapshot) error {
			_, err := s.Search(context.Background(), nil, nil, nil, SearchOptions{})
			return err
		})
		if err != nil {
			t.Errorf("Search: %v", err)
		}
	}()
	<-searchStarted

	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		if _, err := r.Load(context.Background(), Pointer{Version: 2, Key: "k2"}); err != nil {
			t.Errorf("Load v2: %v", err)
		}
	}()

	select {
	case <-loadDone:
		t.Fatal("Load completed while a Search held the read lock")
	case <-time.After(50 * time.Millisecond):
		// expected: Load is blocked on the write lock
	}

	close(releaseSearch)
	<-searchDone

	select {
	case <-loadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Load did not complete after Search released the read lock")
	}
	if !first.closed.Load() {
		t.Fatal("expected v1 to be closed once the v2 swap completed")
	}
}

// TestRuntimeCurrentReflectsLoad exercises Current()'s "nothing loaded" /
// "loaded" transition directly (reload() layers store.current() on top of
// this same load() path; store.current() is covered in s3store_test.go).
func TestRuntimeCurrentReflectsLoad(t *testing.T) {
	first := &fakeSnapshot{manifest: Manifest{Version: 1}}
	fetchFn, openFn := fakeTransport(map[int64]Snapshot{1: first})
	r := NewRuntime(nil, t.TempDir(), Expectations{})
	r.rt.fetchFn, r.rt.openFn = fetchFn, openFn

	if _, ok := r.Current(); ok {
		t.Fatal("nothing loaded yet")
	}
	if _, err := r.Load(context.Background(), Pointer{Version: 1}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cur, ok := r.Current()
	if !ok || cur.Version != 1 {
		t.Fatalf("Current() = %+v, %v", cur, ok)
	}
}
