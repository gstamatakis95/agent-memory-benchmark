package snapshot

import "context"

// STUB — replaced by s3store.go / runtime.go.

type runtimeState struct{}

func (st *S3Store) publish(ctx context.Context, version int64, tarPath string) (string, error) {
	panic("snapshot: not implemented")
}
func (st *S3Store) flipPointer(ctx context.Context, p Pointer) error {
	panic("snapshot: not implemented")
}
func (st *S3Store) current(ctx context.Context) (Pointer, bool, error) {
	panic("snapshot: not implemented")
}
func (st *S3Store) fetch(ctx context.Context, p Pointer, cacheDir string) (string, error) {
	panic("snapshot: not implemented")
}
func newRuntime(store *S3Store, cacheDir string, expect Expectations) *Runtime {
	panic("snapshot: not implemented")
}
func (r *Runtime) reload(ctx context.Context) (Manifest, bool, error) {
	panic("snapshot: not implemented")
}
func (r *Runtime) load(ctx context.Context, p Pointer) (Manifest, error) {
	panic("snapshot: not implemented")
}
func (r *Runtime) currentManifest() (Manifest, bool) { panic("snapshot: not implemented") }
func (r *Runtime) search(ctx context.Context, fn func(Snapshot) error) error {
	panic("snapshot: not implemented")
}
func (r *Runtime) close() error { panic("snapshot: not implemented") }
