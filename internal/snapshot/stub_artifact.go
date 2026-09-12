package snapshot

// STUB — replaced by builder.go / open.go / seal.go / search.go.

func newBuilder(dir string, m Manifest) (Builder, error) { panic("snapshot: not implemented") }
func open(dir string) (Snapshot, error)                  { panic("snapshot: not implemented") }
func seal(dir, tarPath string) error                     { panic("snapshot: not implemented") }
func unseal(tarPath, dir string) error                   { panic("snapshot: not implemented") }
