package store

import (
	"context"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
)

// Inserts groups the insert-only content writers (N113): no method of any of them updates a content row.
type Inserts interface {
	Documents() DocumentWriter
	Chunks() ChunkWriter
	Facts() FactWriter
	Links() LinkWriter
	Entities() EntityWriter
}

// DocumentWriter writes document and version rows.
type DocumentWriter interface {
	// BeginVersion runs under the documents row lock (N56): it assigns the version (above any tombstone's
	// up_to_version) and `append_base_version`; it revives a deleting document at up_to_version + 1 (N133c).
	BeginVersion(ctx context.Context, d DocumentDraft) (id.DocVersion, error)
	Activate(ctx context.Context, doc id.DocumentID, v id.DocVersion) error
	// MarkDeleting is the ONE DELETE_DOCUMENT marker tx (section 3, 5.4.1): it compares expected under the lock, sets
	// state = 'deleting' and clears summary_blob_key, summary_hash, document_hash, context (''), metadata ('{}') and
	// tags in the same row update; a repeat on a deleting document returns the existing operation, not NOT_FOUND; the
	// outbox seq is drawn in the final statement (N115, N136, N157).
	MarkDeleting(ctx context.Context, doc id.DocumentID, expected id.DocVersion,
		at time.Time) (current id.DocVersion, err error)
	// SetBody is guarded by WHERE body_key IS NULL (N104).
	SetBody(ctx context.Context, doc id.DocumentID, v id.DocVersion, key string, sum [32]byte) error
	// Lock: LockShared is a try-lock (errs.DocumentBusy, 100 ms); LockExclusive is one 35 s attempt, generated from the
	// GUC table (N83, N139).
	Lock(ctx context.Context, doc id.DocumentID, m LockMode) error
}

// ChunkWriter writes chunk rows.
type ChunkWriter interface {
	// Insert is ON CONFLICT DO NOTHING; ordinals live in document_version_chunks.
	Insert(ctx context.Context, c ChunkInsert) (id.ChunkID, bool /*inserted*/, error)
	// AddVector writes chunk_vectors keyed with embedding_effective_at; ReembedChunk adds a NEW row (N111).
	AddVector(ctx context.Context, c id.ChunkID, v VectorRow) error
	Member(ctx context.Context, doc id.DocumentID, v id.DocVersion, h [32]byte, c id.ChunkID, ordinal int) error
}

// FactWriter writes fact rows.
type FactWriter interface {
	// Insert writes facts + fact_vectors (document_id, chunk_id, mentioned_at, fact_type copied, N138); unique (chunk,
	// extraction_key, content_hash).
	Insert(ctx context.Context, fs []Fact, vs []VectorRow) error
	// Stamp writes fact_consolidation, insert-only (N95).
	Stamp(ctx context.Context, batch [32]byte, fs []id.FactID, note StampNote) error
}

// LinkWriter inserts ON CONFLICT DO NOTHING in key order; undirected edges once with src < dst.
type LinkWriter interface {
	Insert(ctx context.Context, ls []Link) error
}

// EntityWriter writes entities, aliases and mentions.
type EntityWriter interface {
	// UpsertSorted is one statement, canonical_norm order (N69).
	UpsertSorted(ctx context.Context, es []EntityUpsert) ([]id.EntityID, error)
	AddAliases(ctx context.Context, as []Alias) error    // with the producing document_id (N118)
	AddMentions(ctx context.Context, ms []Mention) error // with the fact's mentioned_at (N118)
}

// DocumentReader reads documents.
type DocumentReader interface {
	// Get: a DELETING document is its content-free tombstone view; covered versions are never listed (N136, N157).
	Get(ctx context.Context, doc id.DocumentID) (*Document, error)
	// List: a tombstone never matches a non-empty tag or metadata filter (A-14).
	List(ctx context.Context, q DocumentQuery) ([]Document, string, error)
	// Version is GetDocumentVersion; NOT_FOUND{DOCUMENT_DELETED} for a covered version.
	Version(ctx context.Context, doc id.DocumentID, v id.DocVersion) (*DocVersion, error)
	Bodies() DocumentBodies // GetDocumentBody
}

// DocumentBodies returns the owner-keyed body key and hash; the API reads the bytes through the shard handle's
// blob.Store.
type DocumentBodies interface {
	Ref(ctx context.Context, doc id.DocumentID, v id.DocVersion) (BodyRef, error)
}

// TagLister lists tags from the per-namespace tag_counts table.
type TagLister interface {
	List(ctx context.Context, q TagQuery) ([]TagCount, string, error)
}

// DocumentTags is Writes.Tags(): the transaction bumps tag_generation, maintains tag_counts and appends
// DocumentTagsUpdated last.
type DocumentTags interface {
	TagLister
	// Update returns NOT_FOUND{DOCUMENT_DELETED} on a deleting document.
	Update(ctx context.Context, doc id.DocumentID, p TagPatch) (*Document, error)
}

// ChunkReader reads chunks.
type ChunkReader interface {
	// Plan classifies each hash: member | live | tombstoned | stale_extraction | absent.
	Plan(ctx context.Context, doc id.DocumentID, hashes [][32]byte) ([]ChunkState, error)
	Membership(ctx context.Context, doc id.DocumentID, v id.DocVersion) ([]id.ChunkID, error)
	Get(ctx context.Context, c id.ChunkID) (*Chunk, error)
}

// FactReader reads facts.
type FactReader interface {
	// ByIDs applies the visibility predicate and as_of; an invalidated fact is FOUND with InvalidatedAt set (GetMemory,
	// BatchGetMemories, N157).
	ByIDs(ctx context.Context, ids []id.FactID, o ReadOptions) ([]Fact, error)
	// NearestByOccurrence is the temporal arm, a two-sided btree probe (N68).
	NearestByOccurrence(ctx context.Context, anchor time.Time, w *memoryv1.TemporalWindow, n int,
		f Filter) ([]Fact, error)
	// Pending is engram_pending_facts: visible, above the watermark, no 'done' stamp (N95).
	Pending(ctx context.Context, limit int) ([]Fact, error)
	Lister() FactLister // ListMemories
}

// FactLister is structural filtering, no ranking; an invalidated fact is listed only on request.
type FactLister interface {
	List(ctx context.Context, q MemoryQuery) ([]Fact, string, error)
}

// GraphReader reads links and entities.
type GraphReader interface {
	// Hop is ONE statement per hop (N165): UNION ALL of `src_memory_id = ANY (frontier)` on the PK and `dst_memory_id =
	// ANY (frontier)` on fact_links_reverse_idx (causal edges forward only), both Index Only Scans; ordered and limited
	// per frontier node BEFORE the facts visibility join, which requires BOTH endpoints visible (N116).
	Hop(ctx context.Context, frontier []id.FactID, kinds []LinkKind, perNode int, f Filter) ([]Link, error)
	// Similar goes through engram_entity_fuzzy (SECURITY DEFINER, N131).
	Similar(ctx context.Context, name, typ string, min float32) ([]EntityCandidate, error)
}
