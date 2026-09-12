package snapshot

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ---------------------------------------------------------------------
// Contract: the functions below are the package's public surface. Their
// bodies live in builder.go / open.go / search.go / seal.go (artifact) and
// s3store.go / runtime.go (transport). This file only declares what other
// packages compile against.
// ---------------------------------------------------------------------

// NewBuilder creates dir (must not already contain a snapshot) and starts a
// Bleve scorch index inside it with BM25 scoring and a whitespace-only
// analyzer over the pre-tokenized lexemes field. m.VectorDim must be > 0;
// m.DocCount is filled in by Close. m.BleveVersion is filled in from the
// linked Bleve module if empty.
func NewBuilder(dir string, m Manifest) (Builder, error) { return newBuilder(dir, m) }

// Open opens a snapshot directory read-only: parses the manifest, loads
// vectors.f32 into a contiguous []float32, ordinals.txt and docs.jsonl into
// RAM, opens index.bleve read-only, and asserts every count equals
// manifest.doc_count and the VEC1 header matches (dim, count).
func Open(dir string) (Snapshot, error) { return open(dir) }

// Seal archives a finished snapshot directory (Builder.Close must have
// returned) into a zstd-compressed tarball at tarPath. Entries are written
// with paths relative to dir, in sorted order, so identical directories
// produce identical archives.
func Seal(dir, tarPath string) error { return seal(dir, tarPath) }

// Unseal extracts a tar.zst produced by Seal into dir (created if needed).
// It rejects entries that escape dir.
func Unseal(tarPath, dir string) error { return unseal(tarPath, dir) }

// Pointer is current.json: the version currently published and the
// artifact key it lives under.
type Pointer struct {
	Version int64  `json:"version"`
	Key     string `json:"key"`
}

// S3Store publishes and fetches artifacts (MinIO in compose).
type S3Store struct {
	Client *s3.Client
	Bucket string
}

// Publish uploads the sealed tarball to ArtifactKey(version). Artifacts
// are immutable: if the key already exists the upload is refused with an
// error (never overwritten). Returns the key.
func (st *S3Store) Publish(ctx context.Context, version int64, tarPath string) (string, error) {
	return st.publish(ctx, version, tarPath)
}

// FlipPointer writes current.json. Callers MUST only call it after Publish
// returned successfully for p.Key (the atomic cutover, docs/07 section 3).
func (st *S3Store) FlipPointer(ctx context.Context, p Pointer) error { return st.flipPointer(ctx, p) }

// Current reads current.json; ok is false when no snapshot was ever
// published.
func (st *S3Store) Current(ctx context.Context) (p Pointer, ok bool, err error) {
	return st.current(ctx)
}

// Fetch downloads the artifact at key into cacheDir/v<version>/ (creating
// it) and extracts it, returning the extracted directory. It is cached by
// the immutable version key: if the directory already holds a manifest
// whose Version matches, nothing is downloaded. A partial download or
// extraction never leaves a directory that looks complete (write to a
// temporary sibling, rename on success).
func (st *S3Store) Fetch(ctx context.Context, p Pointer, cacheDir string) (string, error) {
	return st.fetch(ctx, p, cacheDir)
}

// Runtime is the serving side: it holds the currently loaded Snapshot and
// swaps it atomically on Reload. Searches take a read lock for their
// duration so a swap never closes a snapshot mid-query.
type Runtime struct {
	store    *S3Store
	cacheDir string
	// Expect are the invariants a loaded snapshot must satisfy; a mismatch
	// refuses the load (docs/07 section 7: a silently different embedding
	// space degrades the dense arm to noise with no error anywhere).
	Expect Expectations

	rt runtimeState
}

// Expectations pins what a snapshot must have been built with.
type Expectations struct {
	VectorDim  int    // 0 = don't check
	EmbedModel string // "" = don't check
}

// NewRuntime creates an empty runtime (no snapshot loaded).
func NewRuntime(store *S3Store, cacheDir string, expect Expectations) *Runtime {
	return newRuntime(store, cacheDir, expect)
}

// Reload reads the current pointer and loads that version if it differs
// from the loaded one. loaded=false with err=nil means nothing is
// published yet.
func (r *Runtime) Reload(ctx context.Context) (m Manifest, loaded bool, err error) {
	return r.reload(ctx)
}

// Load fetches and opens the given published version and swaps it in.
func (r *Runtime) Load(ctx context.Context, p Pointer) (Manifest, error) { return r.load(ctx, p) }

// Current returns the loaded manifest; ok is false when nothing is loaded.
func (r *Runtime) Current() (m Manifest, ok bool) { return r.currentManifest() }

// Search runs fn against the loaded snapshot under the read lock. It
// returns an error when nothing is loaded.
func (r *Runtime) Search(ctx context.Context, fn func(Snapshot) error) error {
	return r.search(ctx, fn)
}

// Close closes the loaded snapshot, if any.
func (r *Runtime) Close() error { return r.close() }

// ---------------------------------------------------------------------

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func errorf(format string, a ...any) error { return fmt.Errorf(format, a...) }
