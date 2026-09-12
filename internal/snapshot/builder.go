package snapshot

// Builder side of the artifact: writes index.bleve, vectors.f32,
// ordinals.txt and docs.jsonl in lockstep so row i means the same memory in
// all four artifacts (package doc comment). See docs/01-retrieval.md
// section 4.3/4.7 for why BM25 (Bleve) + brute-force cosine is the chosen
// classical-IR pair.

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	index "github.com/blevesearch/bleve_index_api"
)

// builder implements Builder. Fields are only touched from the goroutine
// that owns the Builder — there is no concurrency contract here (unlike
// Snapshot, which is read concurrently once open).
type builder struct {
	dir string
	m   Manifest

	idx      bleve.Index
	batch    *bleve.Batch
	batchLen int

	vecFile *os.File
	vecW    *bufio.Writer
	ordFile *os.File
	ordW    *bufio.Writer
	docFile *os.File
	docW    *bufio.Writer

	seen    map[int64]struct{}
	count   int64
	lexemes int64   // running total of lexemes, for manifest.LexemeCount
	vecEl   [4]byte // scratch for one little-endian float32

	closed bool
}

// newBuilder is NewBuilder's implementation (see api.go for the contract).
func newBuilder(dir string, m Manifest) (Builder, error) {
	if m.VectorDim <= 0 {
		return nil, errorf("snapshot: VectorDim must be > 0, got %d", m.VectorDim)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err == nil {
		return nil, errorf("snapshot: %s already contains a snapshot (manifest.json exists)", dir)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	idx, err := newBleveIndex(filepath.Join(dir, IndexDir))
	if err != nil {
		return nil, err
	}

	b := &builder{dir: dir, m: m, idx: idx, seen: make(map[int64]struct{})}
	b.batch = idx.NewBatch()

	if b.vecFile, err = os.Create(filepath.Join(dir, VectorsFile)); err != nil {
		b.abort()
		return nil, err
	}
	b.vecW = bufio.NewWriter(b.vecFile)
	if err := writeVectorHeader(b.vecW, m.VectorDim, 0); err != nil {
		b.abort()
		return nil, err
	}

	if b.ordFile, err = os.Create(filepath.Join(dir, OrdinalsFile)); err != nil {
		b.abort()
		return nil, err
	}
	b.ordW = bufio.NewWriter(b.ordFile)

	if b.docFile, err = os.Create(filepath.Join(dir, DocsFile)); err != nil {
		b.abort()
		return nil, err
	}
	b.docW = bufio.NewWriter(b.docFile)

	return b, nil
}

// abort best-effort closes whatever was opened so far; used only on the
// construction error path above, where returning an error means the
// directory is left in a broken (never-Close()d, so never a valid
// snapshot) state anyway.
func (b *builder) abort() {
	if b.idx != nil {
		_ = b.idx.Close()
	}
	for _, f := range []*os.File{b.vecFile, b.ordFile, b.docFile} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// newBleveIndex builds the mapping described in the package doc: BM25
// scoring, a whitespace-only "lexeme" analyzer over pre-tokenized terms (no
// stemming/stopwording in Bleve — pipeline.Tokenize already did that), and
// an exact-match keyword analyzer for conversation_id filtering. Everything
// dynamic is disabled: the two fields above are the entire schema.
func newBleveIndex(path string) (bleve.Index, error) {
	im := bleve.NewIndexMapping()
	im.ScoringModel = index.BM25Scoring

	if err := im.AddCustomAnalyzer("lexeme", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{},
	}); err != nil {
		return nil, err
	}

	lexemes := bleve.NewTextFieldMapping()
	lexemes.Analyzer = "lexeme"
	lexemes.Store = false
	lexemes.IncludeInAll = false
	lexemes.IncludeTermVectors = false
	lexemes.DocValues = false

	conv := bleve.NewTextFieldMapping()
	conv.Analyzer = keyword.Name
	conv.Store = false
	conv.IncludeInAll = false

	im.IndexDynamic = false
	im.StoreDynamic = false
	im.DocValuesDynamic = false
	im.DefaultMapping.Dynamic = false
	im.DefaultMapping.AddFieldMappingsAt("lexemes", lexemes)
	im.DefaultMapping.AddFieldMappingsAt("conversation_id", conv)

	return bleve.New(path, im)
}

// writeVectorHeader writes the fixed VEC1 header (magic, dim, count,
// reserved). Called once at open with count 0, then the count word is
// patched in place at Close once the true row count is known — vectors.f32
// is written strictly append-only in between so nothing else moves.
func writeVectorHeader(w *bufio.Writer, dim int, count uint32) error {
	var hdr [VecHeaderSize]byte
	copy(hdr[0:4], VecMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(hdr[8:12], count)
	_, err := w.Write(hdr[:])
	return err
}

func (b *builder) Count() int64 { return b.count }

// Add validates and appends one row to all four artifacts. See the
// Builder.Add doc comment in snapshot.go for the contract.
func (b *builder) Add(doc Doc, lexemes []string, vec []float32) error {
	if b.closed {
		return errorf("snapshot: Add called after Close")
	}
	if len(vec) != b.m.VectorDim {
		return errorf("snapshot: vector has dim %d, want %d (memory %d)", len(vec), b.m.VectorDim, doc.MemoryID)
	}
	if _, dup := b.seen[doc.MemoryID]; dup {
		return errorf("snapshot: duplicate memory id %d", doc.MemoryID)
	}

	for _, f := range vec {
		binary.LittleEndian.PutUint32(b.vecEl[:], math.Float32bits(f))
		if _, err := b.vecW.Write(b.vecEl[:]); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(b.ordW, "%d\n", doc.MemoryID); err != nil {
		return err
	}

	line, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if _, err := b.docW.Write(line); err != nil {
		return err
	}
	if err := b.docW.WriteByte('\n'); err != nil {
		return err
	}

	docID := fmt.Sprintf("%020d", doc.MemoryID)
	data := map[string]interface{}{
		"lexemes":         strings.Join(lexemes, " "),
		"conversation_id": doc.ConversationID,
	}
	if err := b.batch.Index(docID, data); err != nil {
		return err
	}
	b.batchLen++
	if b.batchLen >= BatchSize {
		if err := b.idx.Batch(b.batch); err != nil {
			return err
		}
		b.batch = b.idx.NewBatch()
		b.batchLen = 0
	}

	b.seen[doc.MemoryID] = struct{}{}
	b.count++
	b.lexemes += int64(len(lexemes))
	return nil
}

// Close finalizes the directory: flush the last batch, close the Bleve
// index (required before archiving — scorch holds file locks and buffers
// segments in memory until Close), patch the vector-file row count, flush
// and close the flat files, write manifest.json, then re-open the result
// to catch any of the invariants Open checks before a caller ever seals a
// broken snapshot.
func (b *builder) Close() (Manifest, error) {
	if b.closed {
		return Manifest{}, errorf("snapshot: Close called twice")
	}
	b.closed = true

	if b.batchLen > 0 {
		if err := b.idx.Batch(b.batch); err != nil {
			return Manifest{}, err
		}
	}
	if err := b.idx.Close(); err != nil {
		return Manifest{}, err
	}

	if err := b.vecW.Flush(); err != nil {
		return Manifest{}, err
	}
	if _, err := b.vecFile.Seek(8, 0); err != nil {
		return Manifest{}, err
	}
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(b.count))
	if _, err := b.vecFile.Write(countBuf[:]); err != nil {
		return Manifest{}, err
	}
	if err := b.vecFile.Close(); err != nil {
		return Manifest{}, err
	}

	if err := b.ordW.Flush(); err != nil {
		return Manifest{}, err
	}
	if err := b.ordFile.Close(); err != nil {
		return Manifest{}, err
	}
	if err := b.docW.Flush(); err != nil {
		return Manifest{}, err
	}
	if err := b.docFile.Close(); err != nil {
		return Manifest{}, err
	}

	m := b.m
	m.DocCount = b.count
	m.LexemeCount = b.lexemes
	m.Metric = Metric
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	} else {
		m.CreatedAt = m.CreatedAt.UTC()
	}
	if m.BleveVersion == "" {
		m.BleveVersion = bleveVersion()
	}

	mf, err := os.Create(filepath.Join(b.dir, ManifestFile))
	if err != nil {
		return Manifest{}, err
	}
	enc := json.NewEncoder(mf)
	enc.SetIndent("", "  ")
	encErr := enc.Encode(m)
	closeErr := mf.Close()
	if encErr != nil {
		return Manifest{}, encErr
	}
	if closeErr != nil {
		return Manifest{}, closeErr
	}

	// Verify: a snapshot that fails to re-open is a bug in the builder, not
	// something a caller should discover only when the runtime later loads
	// the sealed artifact.
	snap, err := open(b.dir)
	if err != nil {
		return Manifest{}, fmt.Errorf("snapshot: built snapshot failed verification open: %w", err)
	}
	if err := snap.Close(); err != nil {
		return Manifest{}, err
	}

	return m, nil
}

// BleveVersion is the version string NewBuilder fills manifest.BleveVersion
// with when the caller leaves it empty — exported so callers/tests can
// compare against it without re-deriving it.
func BleveVersion() string { return bleveVersion() }

// bleveVersion reads the linked bleve module version from build info
// (populated for both `go build` binaries and `go test` binaries) so the
// manifest records exactly what produced the index, without hand-pinning a
// version string that could drift from go.mod.
func bleveVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path == "github.com/blevesearch/bleve/v2" {
			return dep.Version
		}
	}
	return "unknown"
}
