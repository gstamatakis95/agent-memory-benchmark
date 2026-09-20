package snapshot

// Read-only side of the artifact. Open loads vectors.f32, ordinals.txt and
// docs.jsonl fully into RAM (these are per-snapshot, not per-request — the
// runtime holds exactly one loaded snapshot at a time, docs/01-retrieval.md
// section 4.7) and opens index.bleve read-only. Everything below assumes
// the three row-count invariants documented on the package: row i of each
// artifact describes the same memory.

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2"
)

type snap struct {
	dir string
	m   Manifest

	vectors []float32 // row i at [i*dim : (i+1)*dim]
	dim     int

	ordinals []int64 // ordinal -> memory id
	ordByID  map[int64]int

	docs []Doc

	idx bleve.Index

	// convOrds is built eagerly at Open: it is small (one bitmap per
	// conversation seen in the snapshot) and every hybrid query needs it,
	// so there's no lazy-init race to worry about once Open returns.
	convOrds map[string]*roaring.Bitmap
}

// open is Open's implementation (see api.go for the contract).
func open(dir string) (Snapshot, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}

	vectors, err := readVectors(filepath.Join(dir, VectorsFile), m)
	if err != nil {
		return nil, err
	}

	ordinals, ordByID, err := readOrdinals(filepath.Join(dir, OrdinalsFile))
	if err != nil {
		return nil, err
	}

	docs, err := readDocs(filepath.Join(dir, DocsFile))
	if err != nil {
		return nil, err
	}

	// Every artifact must agree with the manifest's doc count — this is the
	// only integrity check tying the four files together (package doc).
	// vectors.f32's own count is already checked against m.DocCount inside
	// readVectors, alongside the exact file-size check.
	n := int(m.DocCount)
	switch {
	case len(ordinals) != n:
		return nil, errorf("snapshot: %s has %d rows but manifest.doc_count is %d", OrdinalsFile, len(ordinals), m.DocCount)
	case len(docs) != n:
		return nil, errorf("snapshot: %s has %d rows but manifest.doc_count is %d", DocsFile, len(docs), m.DocCount)
	}

	idx, err := bleve.OpenUsing(filepath.Join(dir, IndexDir), map[string]interface{}{"read_only": true})
	if err != nil {
		return nil, err
	}

	convOrds := make(map[string]*roaring.Bitmap)
	for i, d := range docs {
		if d.ConversationID == "" {
			continue
		}
		bm, ok := convOrds[d.ConversationID]
		if !ok {
			bm = roaring.New()
			convOrds[d.ConversationID] = bm
		}
		bm.Add(uint32(i))
	}

	return &snap{
		dir:      dir,
		m:        m,
		vectors:  vectors,
		dim:      m.VectorDim,
		ordinals: ordinals,
		ordByID:  ordByID,
		docs:     docs,
		idx:      idx,
		convOrds: convOrds,
	}, nil
}

func readManifest(dir string) (Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, fmt.Errorf("snapshot: parsing %s: %w", ManifestFile, err)
	}
	return m, nil
}

// readVectors loads the VEC1 file whole and decodes it into a contiguous
// []float32, validating the header against the manifest and the file size
// against header+count*dim*4 exactly (a truncated file, or one from a
// different dim, fails here rather than silently reading garbage or
// panicking with an out-of-range index later in VectorTopN).
func readVectors(path string, m Manifest) ([]float32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < VecHeaderSize {
		return nil, errorf("snapshot: %s is %d bytes, shorter than the %d-byte header", VectorsFile, len(data), VecHeaderSize)
	}
	if string(data[0:4]) != VecMagic {
		return nil, errorf("snapshot: %s has bad magic %q, want %q", VectorsFile, data[0:4], VecMagic)
	}
	dim := int(binary.LittleEndian.Uint32(data[4:8]))
	count := int(binary.LittleEndian.Uint32(data[8:12]))
	if dim != m.VectorDim {
		return nil, errorf("snapshot: %s dim %d does not match manifest.vector_dim %d", VectorsFile, dim, m.VectorDim)
	}
	if int64(count) != m.DocCount {
		return nil, errorf("snapshot: %s count %d does not match manifest.doc_count %d", VectorsFile, count, m.DocCount)
	}
	wantSize := VecHeaderSize + count*dim*4
	if len(data) != wantSize {
		return nil, errorf("snapshot: %s is %d bytes, want %d for header+%d rows of dim %d", VectorsFile, len(data), wantSize, count, dim)
	}

	vectors := make([]float32, count*dim)
	rows := data[VecHeaderSize:]
	for i := range vectors {
		vectors[i] = math.Float32frombits(binary.LittleEndian.Uint32(rows[4*i:]))
	}
	return vectors, nil
}

// readOrdinals reads one decimal memory id per line.
func readOrdinals(path string) ([]int64, map[int64]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var ordinals []int64
	ordByID := make(map[int64]int)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		id, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot: parsing %s line %d: %w", OrdinalsFile, len(ordinals)+1, err)
		}
		ordByID[id] = len(ordinals)
		ordinals = append(ordinals, id)
	}
	if err := sc.Err(); err != nil {
		return nil, nil, err
	}
	return ordinals, ordByID, nil
}

// readDocs decodes one JSON object per line via a streaming decoder rather
// than a line scanner, so an unusually long payload (a big S3 key, say)
// never trips a fixed line-length limit.
func readDocs(path string) ([]Doc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var docs []Doc
	dec := json.NewDecoder(f)
	for dec.More() {
		var d Doc
		if err := dec.Decode(&d); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("snapshot: parsing %s row %d: %w", DocsFile, len(docs)+1, err)
		}
		docs = append(docs, d)
	}
	return docs, nil
}

func (s *snap) Manifest() Manifest  { return s.m }
func (s *snap) Len() int            { return len(s.docs) }
func (s *snap) Doc(ordinal int) Doc { return s.docs[ordinal] }

func (s *snap) OrdinalOf(memoryID int64) (int, bool) {
	i, ok := s.ordByID[memoryID]
	return i, ok
}

// ConversationFilter returns nil for "" (no filter) and, for an unknown
// conversation, a Filter with an empty (never nil) bitmap — see the
// Snapshot.ConversationFilter doc comment for why that distinction matters.
func (s *snap) ConversationFilter(conversationID string) *Filter {
	if conversationID == "" {
		return nil
	}
	bm, ok := s.convOrds[conversationID]
	if !ok {
		bm = roaring.New()
	}
	return &Filter{ConversationID: conversationID, Ordinals: bm}
}

func (s *snap) Close() error {
	return s.idx.Close()
}
