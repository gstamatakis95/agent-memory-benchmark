package snapshot

// Builder/Open/Seal/Unseal coverage. Small dims (2-8) throughout — nothing
// here assumes the real nomic 768-dim embedding space.

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/pipeline"
)

// row is one Add() call's worth of input, bundled for table-driven tests.
type row struct {
	Doc     Doc
	Lexemes []string
	Vec     []float32
}

func buildSnapshot(t *testing.T, dir string, m Manifest, rows []row) Manifest {
	t.Helper()
	b, err := NewBuilder(dir, m)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	for _, r := range rows {
		if err := b.Add(r.Doc, r.Lexemes, r.Vec); err != nil {
			t.Fatalf("Add(%d): %v", r.Doc.MemoryID, err)
		}
	}
	mf, err := b.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mf
}

func mustOpen(t *testing.T, dir string) Snapshot {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// randVec returns an L2-normalized random vector, matching how the real
// embedding pipeline hands vectors to Add (internal/embed.L2Normalize).
func randVec(rnd *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rnd.NormFloat64())
	}
	return embed.L2Normalize(v)
}

func TestBuildOpenRoundtrip(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{
			Doc:     Doc{MemoryID: 10, ConversationID: "c1", SessionID: "s1", TSUnix: 1000, ContentHash: []byte{1, 2, 3}, S3Key: "blobs/10"},
			Lexemes: pipeline.Tokenize("the cat sat on the mat"),
			Vec:     embed.L2Normalize([]float32{1, 0, 0, 0}),
		},
		{
			// TSUnix 0 is allowed (content-derived timestamp unknown).
			Doc:     Doc{MemoryID: 20, ConversationID: "c1", SessionID: "s1", TSUnix: 0, ContentHash: []byte{4, 5, 6}, S3Key: "blobs/20"},
			Lexemes: pipeline.Tokenize("a dog ran fast"),
			Vec:     embed.L2Normalize([]float32{0, 1, 0, 0}),
		},
		{
			Doc:     Doc{MemoryID: 5, ConversationID: "c2", SessionID: "s2", TSUnix: 2000, ContentHash: []byte{7, 8, 9}, S3Key: "blobs/5"},
			Lexemes: pipeline.Tokenize("birds fly south"),
			Vec:     embed.L2Normalize([]float32{0, 0, 1, 0}),
		},
	}
	m := Manifest{VectorDim: 4, EmbedModel: "test-embed", EnrichmentVersion: 3}

	got := buildSnapshot(t, dir, m, rows)
	if got.DocCount != int64(len(rows)) {
		t.Fatalf("DocCount = %d, want %d", got.DocCount, len(rows))
	}
	if got.Metric != Metric {
		t.Fatalf("Metric = %q, want %q", got.Metric, Metric)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("CreatedAt was not filled in")
	}
	if got.BleveVersion == "" {
		t.Fatal("BleveVersion was not filled in")
	}

	s := mustOpen(t, dir)
	if s.Len() != len(rows) {
		t.Fatalf("Len() = %d, want %d", s.Len(), len(rows))
	}
	if mf := s.Manifest(); mf.DocCount != got.DocCount || mf.EmbedModel != "test-embed" || mf.EnrichmentVersion != 3 {
		t.Fatalf("Manifest() = %+v", mf)
	}
	for _, r := range rows {
		ord, ok := s.OrdinalOf(r.Doc.MemoryID)
		if !ok {
			t.Fatalf("OrdinalOf(%d) not found", r.Doc.MemoryID)
		}
		d := s.Doc(ord)
		if !bytes.Equal(d.ContentHash, r.Doc.ContentHash) {
			t.Fatalf("memory %d: ContentHash = %x, want %x", r.Doc.MemoryID, d.ContentHash, r.Doc.ContentHash)
		}
		if d.S3Key != r.Doc.S3Key || d.TSUnix != r.Doc.TSUnix || d.ConversationID != r.Doc.ConversationID {
			t.Fatalf("memory %d: Doc = %+v, want %+v", r.Doc.MemoryID, d, r.Doc)
		}
	}
	if _, ok := s.OrdinalOf(999_999); ok {
		t.Fatal("OrdinalOf on an unknown memory id should return ok=false")
	}
}

func TestSealUnsealRoundtrip(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1, ConversationID: "c1", ContentHash: []byte{1}, S3Key: "k1"}, Lexemes: []string{"a"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 2, ConversationID: "c1", ContentHash: []byte{2}, S3Key: "k2"}, Lexemes: []string{"b"}, Vec: embed.L2Normalize([]float32{0, 1})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)

	tarA := filepath.Join(t.TempDir(), "a.tar.zst")
	tarB := filepath.Join(t.TempDir(), "b.tar.zst")
	if err := Seal(dir, tarA); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := Seal(dir, tarB); err != nil {
		t.Fatalf("Seal (second time): %v", err)
	}
	bytesA, err := os.ReadFile(tarA)
	if err != nil {
		t.Fatal(err)
	}
	bytesB, err := os.ReadFile(tarB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytesA, bytesB) {
		t.Fatal("sealing the same directory twice produced different archives")
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if err := Unseal(tarA, restored); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	s := mustOpen(t, restored)
	if s.Len() != len(rows) {
		t.Fatalf("restored Len() = %d, want %d", s.Len(), len(rows))
	}
	if _, ok := s.OrdinalOf(1); !ok {
		t.Fatal("restored snapshot missing memory id 1")
	}
}

func TestOpenTruncatedVectors(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1, ContentHash: []byte{1}}, Lexemes: []string{"a"}, Vec: embed.L2Normalize([]float32{1, 0, 0})},
		{Doc: Doc{MemoryID: 2, ContentHash: []byte{2}}, Lexemes: []string{"b"}, Vec: embed.L2Normalize([]float32{0, 1, 0})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 3}, rows)

	path := filepath.Join(dir, VectorsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-4], 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir); err == nil {
		t.Fatal("Open should fail loudly on a truncated vectors.f32")
	}
}

func TestOpenOrdinalsMismatch(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1, ContentHash: []byte{1}}, Lexemes: []string{"a"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 2, ContentHash: []byte{2}}, Lexemes: []string{"b"}, Vec: embed.L2Normalize([]float32{0, 1})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)

	path := filepath.Join(dir, OrdinalsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 ordinal lines, got %d", len(lines))
	}
	if err := os.WriteFile(path, []byte(lines[0]+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir); err == nil {
		t.Fatal("Open should fail loudly when ordinals.txt is short one line")
	}
}

func TestAddValidation(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBuilder(dir, Manifest{VectorDim: 3})
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}

	if err := b.Add(Doc{MemoryID: 1}, []string{"a"}, []float32{1, 0}); err == nil {
		t.Fatal("Add with a wrong-dim vector should error")
	}
	if err := b.Add(Doc{MemoryID: 1}, []string{"a"}, []float32{1, 0, 0}); err != nil {
		t.Fatalf("Add(1): %v", err)
	}
	if err := b.Add(Doc{MemoryID: 1}, []string{"a"}, []float32{0, 1, 0}); err == nil {
		t.Fatal("Add with a duplicate memory id should error")
	}
	if b.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", b.Count())
	}
	if _, err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := NewBuilder(dir, Manifest{VectorDim: 3}); err == nil {
		t.Fatal("NewBuilder on a directory that already has a manifest should error")
	}
}
