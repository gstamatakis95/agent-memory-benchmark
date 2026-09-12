// Package snapshot implements immutable, versioned retrieval snapshots
// (docs/07-snapshot-serving.md): a Bleve (scorch, pure Go) BM25 index plus a
// flat, L2-normalized float32 vector file, built once from the
// version-pinned enrichment ledger, sealed into a tar.zst artifact, published
// to S3 under a content-addressed key, and served by a stateless runtime
// that downloads the artifact to local disk.
//
// Artifact layout (all paths relative to the snapshot directory):
//
//	manifest.json   Manifest
//	index.bleve/    scorch segment directory (BM25 scoring, pre-tokenized
//	                lexemes indexed with a whitespace-only analyzer)
//	vectors.f32     VEC1 header + count*dim little-endian float32 rows
//	ordinals.txt    row i -> memory id (decimal), one per line
//	docs.jsonl      row i -> Doc payload (one JSON object per line)
//
// Row i of vectors.f32, line i of ordinals.txt and line i of docs.jsonl
// describe the same memory; the Bleve document id is the decimal memory id.
// That ordinal mapping is the only link between the lexical and dense arms,
// so Open asserts all three counts agree with manifest.doc_count.
//
// Classical IR only: BM25 (Bleve), brute-force dot product (cosine on
// normalized vectors), Reciprocal Rank Fusion, a rule-based post-fusion
// recency boost, and a score-threshold abstention. No LLMs, no ANN.
package snapshot

import (
	"context"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
)

// Artifact file names.
const (
	ManifestFile = "manifest.json"
	IndexDir     = "index.bleve"
	VectorsFile  = "vectors.f32"
	OrdinalsFile = "ordinals.txt"
	DocsFile     = "docs.jsonl"

	// VecMagic is the 4-byte magic at offset 0 of vectors.f32.
	VecMagic = "VEC1"
	// VecHeaderSize is the fixed header: magic[4] dim(u32 LE) count(u32 LE) reserved[4].
	VecHeaderSize = 16

	// Metric is the only supported similarity: cosine == dot on normalized vectors.
	Metric = "cosine"

	// BatchSize is how many documents one Bleve batch carries. Low thousands:
	// single-document Index() calls create many tiny segments and thrash
	// the merger.
	BatchSize = 1000
)

// S3 key layout: artifacts are content-addressed by version and NEVER
// overwritten; current.json is the atomic pointer flipped strictly after
// the artifact upload completes.
const (
	S3Prefix   = "snapshots/"
	PointerKey = S3Prefix + "current.json"
)

// ArtifactKey is the S3 object key of a snapshot version's tarball.
func ArtifactKey(version int64) string {
	return S3Prefix + "snapshot-v" + itoa(version) + ".tar.zst"
}

// Manifest is manifest.json. Version is the monotonic snapshot version
// (allocated from the Postgres sequence snapshot_version_seq); it is NOT
// the enrichment version, which is recorded separately so a runtime can
// refuse a snapshot built from a different pipeline version.
type Manifest struct {
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	DocCount  int64     `json:"doc_count"`
	// LexemeCount is the total number of lexemes across all rows. Bleve's
	// BM25 length normalization derives the average field length from
	// stats the caller can supply per request; without them it falls back
	// to ceil(distinct terms / doc count), which is ~1 on any real corpus
	// and turns BM25 into "shortest document wins". LexicalTopN feeds
	// ceil(LexemeCount/DocCount) back in so the norm is the real average.
	LexemeCount       int64  `json:"lexeme_count"`
	VectorDim         int    `json:"vector_dim"`
	Metric            string `json:"metric"`
	EmbedModel        string `json:"embed_model"`
	EmbedModelRev     string `json:"embed_model_rev,omitempty"`
	EnrichmentVersion int16  `json:"enrichment_version"`
	BleveVersion      string `json:"bleve_version"`
}

// Doc is the per-row payload (docs.jsonl). The index KEY (lexemes) and the
// vector are not part of the payload; they live in index.bleve and
// vectors.f32 respectively. ContentHash is the sha256 of the source blob,
// which is how the eval client maps a hit back to a dataset turn id.
type Doc struct {
	MemoryID       int64  `json:"memory_id"`
	ConversationID string `json:"conversation_id"`
	SessionID      string `json:"session_id"`
	TSUnix         int64  `json:"ts_unix,omitempty"` // content-derived; 0 = unknown
	ContentHash    []byte `json:"content_hash"`
	S3Key          string `json:"s3_key"`
}

// Hit is one ranked row.
type Hit struct {
	Ordinal  int
	MemoryID int64
	Score    float64
}

// Mode selects which arms run.
type Mode string

const (
	ModeBM25   Mode = "bm25"
	ModeDense  Mode = "dense"
	ModeHybrid Mode = "hybrid"
)

// ParseMode validates a mode string; empty selects hybrid.
func ParseMode(s string) (Mode, error) {
	switch Mode(s) {
	case "":
		return ModeHybrid, nil
	case ModeBM25, ModeDense, ModeHybrid:
		return Mode(s), nil
	}
	return "", errorf("snapshot: unknown mode %q (want bm25|dense|hybrid)", s)
}

// Query-path defaults (docs/07-snapshot-serving.md section 4/5).
const (
	DefaultTopK         = 10
	DefaultDepth        = 40 // per-arm candidate depth before fusion
	DefaultRRFK         = 60
	DefaultHalfLifeDays = 30.0
)

// SearchOptions configures one query. Zero values select the defaults;
// RecencyBoost and AbstainThreshold default to OFF.
type SearchOptions struct {
	Mode  Mode
	TopK  int
	Depth int
	RRFK  int
	// Post-fusion multiplicative recency boost:
	//   final = rrf * (1 + RecencyBoost*exp(-ln2/HalfLifeDays * ageDays))
	// with ageDays measured from Now to the row's timestamp; rows without a
	// timestamp get no boost. RecencyBoost 0 disables it.
	RecencyBoost float64
	HalfLifeDays float64
	// AbstainThreshold: if the top fused (post-boost) score is below it, the
	// result is flagged Abstained (hits are still returned). 0 disables it.
	AbstainThreshold float64
	// Now is the reference time for recency; zero means time.Now().
	Now time.Time
}

// Result is a Search outcome.
type Result struct {
	Hits        []Hit
	Abstained   bool
	LexicalOnly bool // dense arm was skipped (no query vector supplied)
}

// Filter is the single filter definition applied to BOTH arms: the lexical
// arm adds a conjunct term query on conversation_id, the dense arm tests
// Ordinals membership before computing each dot product. A nil *Filter means
// no filtering.
type Filter struct {
	ConversationID string
	Ordinals       *roaring.Bitmap
}

// Builder writes a snapshot directory. Usage: NewBuilder -> Add xN ->
// Close (flushes the last batch, closes the Bleve index — REQUIRED before
// archiving — writes manifest.json and re-opens the result to verify it).
type Builder interface {
	// Add appends one row. len(vec) must equal the manifest's VectorDim
	// and vec must already be L2-normalized; lexemes are the pre-tokenized
	// BM25 terms (pipeline.Tokenize output).
	Add(doc Doc, lexemes []string, vec []float32) error
	// Count is the number of rows added so far.
	Count() int64
	// Close finalizes the directory and returns the completed Manifest.
	Close() (Manifest, error)
}

// Snapshot is an opened, read-only snapshot directory.
type Snapshot interface {
	Manifest() Manifest
	Len() int
	// Doc returns the payload of row ordinal.
	Doc(ordinal int) Doc
	// OrdinalOf maps a memory id to its row.
	OrdinalOf(memoryID int64) (int, bool)
	// ConversationFilter builds the Filter for one conversation id. It
	// returns a Filter whose Ordinals bitmap is empty (not nil) when the
	// conversation has no rows, so callers never accidentally search
	// unfiltered. conversationID "" returns nil (no filter).
	ConversationFilter(conversationID string) *Filter
	// LexicalTopN runs a Bleve BM25 disjunction of the query lexemes
	// (repeated lexemes count once per occurrence) and returns up to n
	// hits, score-descending with a deterministic ascending-memory-id
	// tiebreak. Only rows matching at least one term are returned.
	LexicalTopN(ctx context.Context, lexemes []string, n int, f *Filter) ([]Hit, error)
	// VectorTopN brute-force scans the vector rows (parallelized over
	// GOMAXPROCS row ranges, per-worker min-heaps, filter tested before the
	// dot product) and returns the top n by dot product, same ordering
	// contract as LexicalTopN. len(q) must equal VectorDim.
	VectorTopN(ctx context.Context, q []float32, n int, f *Filter) ([]Hit, error)
	// Search runs the query path of docs/07 section 4: the arms selected by
	// opts.Mode run concurrently, RRF fuses their ranks (single-arm modes
	// keep the arm's native scores), then recency boost and abstention
	// apply, then truncation to TopK. qvec nil with a dense/hybrid mode
	// degrades to lexical-only and sets Result.LexicalOnly (the runtime
	// uses this when the embedder times out).
	Search(ctx context.Context, lexemes []string, qvec []float32, f *Filter, opts SearchOptions) (Result, error)
	Close() error
}

// RRF fuses ranked hit lists (rank 1 = first element):
// RRF(d) = sum over lists of 1/(k + rank). A document absent from a list
// contributes 0 for that list. Keys are ordinals.
func RRF(k int, lists ...[]Hit) map[int]float64 {
	scores := make(map[int]float64)
	for _, list := range lists {
		for i, h := range list {
			scores[h.Ordinal] += 1 / float64(k+i+1)
		}
	}
	return scores
}
