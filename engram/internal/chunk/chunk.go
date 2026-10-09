// Package chunk is the heading-anchored content-defined chunker (PLAN.md section 2.2.9; register N6, N60, N86). Pure
// Go, no I/O: it splits a document into deterministic chunks (target 3,000 chars, min 500, max 4,000, no overlap) at
// headings and then at content-defined cut points, forces a hard boundary between items more than 24 h apart (N86) and
// builds the contextual header `[doc summary <= 200 chars] > [heading path]`. Pattern: Strategy (CDCChunker /
// FixedChunker). `content_hash = sha256(text)` excludes the header (N6), so a chunk's identity survives a new summary;
// the header is embedded into the chunk vector only. M0.1 declares the signatures only.
package chunk

import (
	"context"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
)

// Item is one retained item: a timestamp and its byte span in the document content.
type Item struct {
	Timestamp time.Time
	ByteStart int
	ByteEnd   int
}

// BaseChunk is an existing chunk of the base version, for the APPEND re-chunk.
type BaseChunk struct {
	ContentHash [32]byte
	ByteStart   int
	ByteEnd     int
	MentionedAt time.Time
}

// Document is the chunker input.
type Document struct {
	ID         id.DocumentID
	Content    string
	Items      []Item      // timestamp + byte span
	BaseChunks []BaseChunk // APPEND re-chunk
	Context    string
}

// Options are the chunker options.
type Options struct {
	MaxItemGap                      time.Duration // 24 h
	TargetChars, MinChars, MaxChars int
	Summary, SummaryID              string
}

// Chunk is one output chunk.
type Chunk struct {
	ContentHash [32]byte
	Text        string
	Header      string
	HeadingPath []string
	ByteStart   int
	ByteEnd     int
	Tokens      int
	// ItemIndexes and MentionedAt: the max over the covered items (and, on APPEND, the overlapped base chunks) is the
	// as_of key (N86).
	ItemIndexes []int
	MentionedAt time.Time
}

// Chunked is the chunker output. Timestamp regressions are accepted and clamped up, never rejected.
type Chunked struct {
	Chunks            []Chunk
	TimestampsClamped int
}

// Chunker splits a document.
type Chunker interface {
	Chunk(ctx context.Context, d Document, o Options) (Chunked, error)
}

// HeaderBuilder builds the contextual header.
type HeaderBuilder interface {
	Build(summary string, headingPath []string, docContext string) string
	// HeaderHash is sha256(heading path || summary id), N60.
	HeaderHash(summaryID string, headingPath []string) [32]byte
}

// SummaryPolicy reports whether the document summary is recomputed: REPLACE, or more than 25 % growth (N60).
func SummaryPolicy(mode memoryv1.UpdateMode, prevBytes, newBytes int) (recompute bool) {
	panic("stub")
}
